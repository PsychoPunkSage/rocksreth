//! Optimism block execution strategy.

use crate::{
    l1::ensure_create2_deployer, BasicOpReceiptBuilder, OpBlockExecutionError, OpEvmConfig,
    OpReceiptBuilder, ReceiptBuilderCtx,
};
use alloc::{boxed::Box, sync::Arc, vec::Vec};
use alloy_consensus::{BlockHeader, Eip658Value, Receipt, Transaction as _};
use alloy_eips::eip7685::Requests;
use op_alloy_consensus::OpDepositReceipt;
use reth_chainspec::EthereumHardforks;
use reth_consensus::ConsensusError;
use reth_evm::{
    execute::{
        balance_increment_state, BasicBlockExecutorProvider, BlockExecutionError,
        BlockExecutionStrategy, BlockExecutionStrategyFactory, BlockValidationError, ExecuteOutput,
    },
    state_change::post_block_balance_increments,
    system_calls::{OnStateHook, SystemCaller},
    ConfigureEvmFor, Database, Evm,
};
use reth_optimism_chainspec::OpChainSpec;
use reth_optimism_consensus::validate_block_post_execution;
use reth_optimism_forks::OpHardfork;
use reth_optimism_primitives::{
    transaction::signed::OpTransaction, DepositReceipt, OpPrimitives, OpReceipt,
};
use reth_primitives::{NodePrimitives, RecoveredBlock};
use reth_primitives_traits::{BlockBody, SignedTransaction};
use reth_revm::State;
use revm_primitives::{db::DatabaseCommit, ResultAndState};
use tracing::trace;

/// Factory for [`OpExecutionStrategy`].
#[derive(Debug, Clone)]
pub struct OpExecutionStrategyFactory<N: NodePrimitives = OpPrimitives, EvmConfig = OpEvmConfig> {
    /// The chainspec
    chain_spec: Arc<OpChainSpec>,
    /// How to create an EVM.
    evm_config: EvmConfig,
    /// Receipt builder.
    receipt_builder: Arc<dyn OpReceiptBuilder<N::SignedTx, Receipt = N::Receipt>>,
}

impl OpExecutionStrategyFactory<OpPrimitives> {
    /// Creates a new default optimism executor strategy factory.
    pub fn optimism(chain_spec: Arc<OpChainSpec>) -> Self {
        Self::new(
            chain_spec.clone(),
            OpEvmConfig::new(chain_spec),
            BasicOpReceiptBuilder::default(),
        )
    }
}

impl<N: NodePrimitives, EvmConfig> OpExecutionStrategyFactory<N, EvmConfig> {
    /// Creates a new executor strategy factory.
    pub fn new(
        chain_spec: Arc<OpChainSpec>,
        evm_config: EvmConfig,
        receipt_builder: impl OpReceiptBuilder<N::SignedTx, Receipt = N::Receipt>,
    ) -> Self {
        Self { chain_spec, evm_config, receipt_builder: Arc::new(receipt_builder) }
    }
}

impl<N, EvmConfig> BlockExecutionStrategyFactory for OpExecutionStrategyFactory<N, EvmConfig>
where
    N: NodePrimitives<
        BlockHeader = alloy_consensus::Header,
        Receipt = OpReceipt,
        SignedTx: OpTransaction,
    >,
    EvmConfig: ConfigureEvmFor<N> + Clone + Unpin + Sync + Send + 'static,
{
    type Primitives = N;
    type Strategy<DB: Database> = OpExecutionStrategy<DB, N, EvmConfig>;

    fn create_strategy<DB>(&self, db: DB) -> Self::Strategy<DB>
    where
        DB: Database,
    {
        let state =
            State::builder().with_database(db).with_bundle_update().without_state_clear().build();
        OpExecutionStrategy::new(
            state,
            self.chain_spec.clone(),
            self.evm_config.clone(),
            self.receipt_builder.clone(),
        )
    }
}

/// Block execution strategy for Optimism.
#[allow(missing_debug_implementations)]
pub struct OpExecutionStrategy<DB, N: NodePrimitives, EvmConfig>
where
    EvmConfig: Clone,
{
    /// The chainspec
    chain_spec: Arc<OpChainSpec>,
    /// How to create an EVM.
    evm_config: EvmConfig,
    /// Current state for block execution.
    state: State<DB>,
    /// Utility to call system smart contracts.
    system_caller: SystemCaller<EvmConfig, OpChainSpec>,
    /// Receipt builder.
    receipt_builder: Arc<dyn OpReceiptBuilder<N::SignedTx, Receipt = N::Receipt>>,
}

impl<DB, N, EvmConfig> OpExecutionStrategy<DB, N, EvmConfig>
where
    N: NodePrimitives,
    EvmConfig: Clone,
{
    /// Creates a new [`OpExecutionStrategy`]
    pub fn new(
        state: State<DB>,
        chain_spec: Arc<OpChainSpec>,
        evm_config: EvmConfig,
        receipt_builder: Arc<dyn OpReceiptBuilder<N::SignedTx, Receipt = N::Receipt>>,
    ) -> Self {
        let system_caller = SystemCaller::new(evm_config.clone(), chain_spec.clone());
        Self { state, chain_spec, evm_config, system_caller, receipt_builder }
    }
}

impl<DB, N, EvmConfig> BlockExecutionStrategy for OpExecutionStrategy<DB, N, EvmConfig>
where
    DB: Database,
    N: NodePrimitives<
        BlockHeader = alloy_consensus::Header,
        SignedTx: OpTransaction,
        Receipt: DepositReceipt,
    >,
    EvmConfig: ConfigureEvmFor<N>,
{
    type DB = DB;
    type Primitives = N;
    type Error = BlockExecutionError;

    fn apply_pre_execution_changes(
        &mut self,
        block: &RecoveredBlock<N::Block>,
    ) -> Result<(), Self::Error> {
        // Set state clear flag if the block is after the Spurious Dragon hardfork.
        let state_clear_flag =
            (*self.chain_spec).is_spurious_dragon_active_at_block(block.number());
        self.state.set_state_clear_flag(state_clear_flag);

        let mut evm = self.evm_config.evm_for_block(&mut self.state, block.header());

        self.system_caller.apply_beacon_root_contract_call(
            block.header().timestamp,
            block.header().number,
            block.header().parent_beacon_block_root,
            &mut evm,
        )?;

        // Ensure that the create2deployer is force-deployed at the canyon transition. Optimism
        // blocks will always have at least a single transaction in them (the L1 info transaction),
        // so we can safely assume that this will always be triggered upon the transition and that
        // the above check for empty blocks will never be hit on OP chains.
        ensure_create2_deployer(self.chain_spec.clone(), block.header().timestamp, evm.db_mut())
            .map_err(|_| OpBlockExecutionError::ForceCreate2DeployerFail)?;

        Ok(())
    }

    fn execute_transactions(
        &mut self,
        block: &RecoveredBlock<N::Block>,
    ) -> Result<ExecuteOutput<N::Receipt>, Self::Error> {
        let mut evm = self.evm_config.evm_for_block(&mut self.state, block.header());

        let is_regolith =
            self.chain_spec.fork(OpHardfork::Regolith).active_at_timestamp(block.timestamp());

        let mut cumulative_gas_used = 0;
        let mut receipts = Vec::with_capacity(block.body().transaction_count());
        for (sender, transaction) in block.transactions_with_sender() {
            // The sum of the transaction’s gas limit, Tg, and the gas utilized in this block prior,
            // must be no greater than the block’s gasLimit.
            let block_available_gas = block.gas_limit() - cumulative_gas_used;
            if transaction.gas_limit() > block_available_gas &&
                (is_regolith || !transaction.is_deposit())
            {
                return Err(BlockValidationError::TransactionGasLimitMoreThanAvailableBlockGas {
                    transaction_gas_limit: transaction.gas_limit(),
                    block_available_gas,
                }
                .into())
            }

            // Cache the depositor account prior to the state transition for the deposit nonce.
            //
            // Note that this *only* needs to be done post-regolith hardfork, as deposit nonces
            // were not introduced in Bedrock. In addition, regular transactions don't have deposit
            // nonces, so we don't need to touch the DB for those.
            let depositor = (is_regolith && transaction.is_deposit())
                .then(|| {
                    evm.db_mut()
                        .load_cache_account(*sender)
                        .map(|acc| acc.account_info().unwrap_or_default())
                })
                .transpose()
                .map_err(|_| OpBlockExecutionError::AccountLoadFailed(*sender))?;

            let tx_env = self.evm_config.tx_env(transaction, *sender);

            // Execute transaction.
            let result_and_state = evm.transact(tx_env).map_err(move |err| {
                // Ensure hash is calculated for error log, if not already done
                BlockValidationError::EVM {
                    hash: transaction.recalculate_hash(),
                    error: Box::new(err),
                }
            })?;

            trace!(
                target: "evm",
                ?transaction,
                "Executed transaction"
            );
            self.system_caller.on_state(&result_and_state.state);
            let ResultAndState { result, state } = result_and_state;
            evm.db_mut().commit(state);

            // append gas used
            cumulative_gas_used += result.gas_used();

            receipts.push(
                match self.receipt_builder.build_receipt(ReceiptBuilderCtx {
                    tx: transaction,
                    result,
                    cumulative_gas_used,
                }) {
                    Ok(receipt) => receipt,
                    Err(ctx) => {
                        let receipt = Receipt {
                            // Success flag was added in `EIP-658: Embedding transaction status code
                            // in receipts`.
                            status: Eip658Value::Eip658(ctx.result.is_success()),
                            cumulative_gas_used,
                            logs: ctx.result.into_logs(),
                        };

                        self.receipt_builder.build_deposit_receipt(OpDepositReceipt {
                            inner: receipt,
                            deposit_nonce: depositor.map(|account| account.nonce),
                            // The deposit receipt version was introduced in Canyon to indicate an
                            // update to how receipt hashes should be computed
                            // when set. The state transition process ensures
                            // this is only set for post-Canyon deposit
                            // transactions.
                            deposit_receipt_version: (transaction.is_deposit() &&
                                self.chain_spec.is_fork_active_at_timestamp(
                                    OpHardfork::Canyon,
                                    block.header().timestamp,
                                ))
                            .then_some(1),
                        })
                    }
                },
            );
        }

        Ok(ExecuteOutput { receipts, gas_used: cumulative_gas_used })
    }

    fn apply_post_execution_changes(
        &mut self,
        block: &RecoveredBlock<N::Block>,
        _receipts: &[N::Receipt],
    ) -> Result<Requests, Self::Error> {
        let balance_increments = post_block_balance_increments(&self.chain_spec.clone(), block);
        // increment balances
        self.state
            .increment_balances(balance_increments.clone())
            .map_err(|_| BlockValidationError::IncrementBalanceFailed)?;
        // call state hook with changes due to balance increments.
        let balance_state = balance_increment_state(&balance_increments, &mut self.state)?;
        self.system_caller.on_state(&balance_state);

        Ok(Requests::default())
    }

    fn state_ref(&self) -> &State<DB> {
        &self.state
    }

    fn state_mut(&mut self) -> &mut State<DB> {
        &mut self.state
    }

    fn with_state_hook(&mut self, hook: Option<Box<dyn OnStateHook>>) {
        self.system_caller.with_state_hook(hook);
    }

    fn validate_block_post_execution(
        &self,
        block: &RecoveredBlock<N::Block>,
        receipts: &[N::Receipt],
        _requests: &Requests,
    ) -> Result<(), ConsensusError> {
        validate_block_post_execution(block.header(), &self.chain_spec.clone(), receipts)
    }
}

/// Helper type with backwards compatible methods to obtain executor providers.
#[derive(Debug)]
pub struct OpExecutorProvider;

impl OpExecutorProvider {
    /// Creates a new default optimism executor strategy factory.
    pub fn optimism(
        chain_spec: Arc<OpChainSpec>,
    ) -> BasicBlockExecutorProvider<OpExecutionStrategyFactory<OpPrimitives>> {
        BasicBlockExecutorProvider::new(OpExecutionStrategyFactory::optimism(chain_spec))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OpChainSpec;
    use alloy_consensus::{Header, TxEip1559};
    use alloy_primitives::{
        b256, Address, PrimitiveSignature as Signature, StorageKey, StorageValue, U256,
    };
    use op_alloy_consensus::{OpTypedTransaction, TxDeposit};
    use reth_chainspec::MIN_TRANSACTION_GAS;
    use reth_evm::execute::{BasicBlockExecutorProvider, BatchExecutor, BlockExecutorProvider};
    use reth_optimism_chainspec::OpChainSpecBuilder;
    use reth_optimism_primitives::OpTransactionSigned;
    use reth_primitives::{Account, Block, BlockBody};
    use reth_revm::{
        database::StateProviderDatabase, test_utils::StateProviderTest, L1_BLOCK_CONTRACT,
    };
    use std::{collections::HashMap, str::FromStr};

    fn create_op_state_provider() -> StateProviderTest {
        let mut db = StateProviderTest::default();

        let l1_block_contract_account =
            Account { balance: U256::ZERO, bytecode_hash: None, nonce: 1 };

        let mut l1_block_storage = HashMap::default();
        // base fee
        l1_block_storage.insert(StorageKey::with_last_byte(1), StorageValue::from(1000000000));
        // l1 fee overhead
        l1_block_storage.insert(StorageKey::with_last_byte(5), StorageValue::from(188));
        // l1 fee scalar
        l1_block_storage.insert(StorageKey::with_last_byte(6), StorageValue::from(684000));
        // l1 free scalars post ecotone
        l1_block_storage.insert(
            StorageKey::with_last_byte(3),
            StorageValue::from_str(
                "0x0000000000000000000000000000000000001db0000d27300000000000000005",
            )
            .unwrap(),
        );

        db.insert_account(L1_BLOCK_CONTRACT, l1_block_contract_account, None, l1_block_storage);

        db
    }

    fn executor_provider(
        chain_spec: Arc<OpChainSpec>,
    ) -> BasicBlockExecutorProvider<OpExecutionStrategyFactory> {
        let strategy_factory = OpExecutionStrategyFactory::optimism(chain_spec);

        BasicBlockExecutorProvider::new(strategy_factory)
    }

    #[test]
    fn op_deposit_fields_pre_canyon() {
        let header = Header {
            timestamp: 1,
            number: 1,
            gas_limit: 1_000_000,
            gas_used: 42_000,
            receipts_root: b256!(
                "83465d1e7d01578c0d609be33570f91242f013e9e295b0879905346abbd63731"
            ),
            ..Default::default()
        };

        let mut db = create_op_state_provider();

        let addr = Address::ZERO;
        let account = Account { balance: U256::MAX, ..Account::default() };
        db.insert_account(addr, account, None, HashMap::default());

        let chain_spec = Arc::new(OpChainSpecBuilder::base_mainnet().regolith_activated().build());

        let tx = OpTransactionSigned::new_unhashed(
            OpTypedTransaction::Eip1559(TxEip1559 {
                chain_id: chain_spec.chain.id(),
                nonce: 0,
                gas_limit: MIN_TRANSACTION_GAS,
                to: addr.into(),
                ..Default::default()
            }),
            Signature::test_signature(),
        );

        let tx_deposit = OpTransactionSigned::new_unhashed(
            OpTypedTransaction::Deposit(op_alloy_consensus::TxDeposit {
                from: addr,
                to: addr.into(),
                gas_limit: MIN_TRANSACTION_GAS,
                ..Default::default()
            }),
            Signature::test_signature(),
        );

        let provider = executor_provider(chain_spec);
        let mut executor = provider.batch_executor(StateProviderDatabase::new(&db));

        // make sure the L1 block contract state is preloaded.
        executor.with_state_mut(|state| {
            state.load_cache_account(L1_BLOCK_CONTRACT).unwrap();
        });

        // Attempt to execute a block with one deposit and one non-deposit transaction
        executor
            .execute_and_verify_one(&RecoveredBlock::new_unhashed(
                Block {
                    header,
                    body: BlockBody { transactions: vec![tx, tx_deposit], ..Default::default() },
                },
                vec![addr, addr],
            ))
            .unwrap();

        let receipts = executor.receipts();
        let tx_receipt = &receipts[0][0];
        let deposit_receipt = &receipts[0][1];

        assert!(!matches!(tx_receipt, OpReceipt::Deposit(_)));
        // deposit_nonce is present only in deposit transactions
        let OpReceipt::Deposit(deposit_receipt) = deposit_receipt else {
            panic!("expected deposit")
        };
        assert!(deposit_receipt.deposit_nonce.is_some());
        // deposit_receipt_version is not present in pre canyon transactions
        assert!(deposit_receipt.deposit_receipt_version.is_none());
    }

    #[test]
    fn op_deposit_fields_post_canyon() {
        // ensure_create2_deployer will fail if timestamp is set to less then 2
        let header = Header {
            timestamp: 2,
            number: 1,
            gas_limit: 1_000_000,
            gas_used: 42_000,
            receipts_root: b256!(
                "fffc85c4004fd03c7bfbe5491fae98a7473126c099ac11e8286fd0013f15f908"
            ),
            ..Default::default()
        };

        let mut db = create_op_state_provider();
        let addr = Address::ZERO;
        let account = Account { balance: U256::MAX, ..Account::default() };

        db.insert_account(addr, account, None, HashMap::default());

        let chain_spec = Arc::new(OpChainSpecBuilder::base_mainnet().canyon_activated().build());

        let tx = OpTransactionSigned::new_unhashed(
            OpTypedTransaction::Eip1559(TxEip1559 {
                chain_id: chain_spec.chain.id(),
                nonce: 0,
                gas_limit: MIN_TRANSACTION_GAS,
                to: addr.into(),
                ..Default::default()
            }),
            Signature::test_signature(),
        );

        let tx_deposit = OpTransactionSigned::new_unhashed(
            OpTypedTransaction::Deposit(op_alloy_consensus::TxDeposit {
                from: addr,
                to: addr.into(),
                gas_limit: MIN_TRANSACTION_GAS,
                ..Default::default()
            }),
            TxDeposit::signature(),
        );

        let provider = executor_provider(chain_spec);
        let mut executor = provider.batch_executor(StateProviderDatabase::new(&db));

        // make sure the L1 block contract state is preloaded.
        executor.with_state_mut(|state| {
            state.load_cache_account(L1_BLOCK_CONTRACT).unwrap();
        });

        // attempt to execute an empty block with parent beacon block root, this should not fail
        executor
            .execute_and_verify_one(&RecoveredBlock::new_unhashed(
                Block {
                    header,
                    body: BlockBody { transactions: vec![tx, tx_deposit], ..Default::default() },
                },
                vec![addr, addr],
            ))
            .expect("Executing a block while canyon is active should not fail");

        let receipts = executor.receipts();
        let tx_receipt = &receipts[0][0];
        let deposit_receipt = &receipts[0][1];

        // deposit_receipt_version is set to 1 for post canyon deposit transactions
        assert!(!matches!(tx_receipt, OpReceipt::Deposit(_)));
        let OpReceipt::Deposit(deposit_receipt) = deposit_receipt else {
            panic!("expected deposit")
        };
        assert_eq!(deposit_receipt.deposit_receipt_version, Some(1));

        // deposit_nonce is present only in deposit transactions
        assert!(deposit_receipt.deposit_nonce.is_some());
    }
}
