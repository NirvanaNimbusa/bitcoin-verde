package com.softwareverde.bitcoin.server.module.node.sync;

import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockInflater;
import com.softwareverde.bitcoin.context.core.BlockProcessorContext;
import com.softwareverde.bitcoin.context.core.BlockchainBuilderContext;
import com.softwareverde.bitcoin.context.core.PendingBlockLoaderContext;
import com.softwareverde.bitcoin.inflater.BlockInflaters;
import com.softwareverde.bitcoin.inflater.TransactionInflaters;
import com.softwareverde.bitcoin.server.module.node.BlockProcessor;
import com.softwareverde.bitcoin.server.module.node.database.block.pending.fullnode.FullNodePendingBlockDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.fullnode.FullNodeDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.sync.blockloader.PendingBlockLoader;
import com.softwareverde.bitcoin.slp.validator.TransactionAccumulator;
import com.softwareverde.bitcoin.test.BlockData;
import com.softwareverde.bitcoin.test.FakeBlockStore;
import com.softwareverde.bitcoin.test.IntegrationTest;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.concurrent.service.SleepyService;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.immutable.ImmutableList;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.network.time.MutableNetworkTime;
import com.softwareverde.util.HexUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class SlpTransactionProcessorAccumulatorTests extends IntegrationTest {
    @Test
    public void accumulator_should_load_existing_confirmed_transaction() throws Exception {
        // Setup
        final BlockInflater blockInflater = _masterInflater.getBlockInflater();
        final TransactionInflaters transactionInflaters = _masterInflater;
        final FakeBlockStore blockStore = new FakeBlockStore();
        final BlockchainBuilderTests.FakeBitcoinNodeManager bitcoinNodeManager = new BlockchainBuilderTests.FakeBitcoinNodeManager();
        final BlockInflaters blockInflaters = BlockchainBuilderTests.FAKE_BLOCK_INFLATERS;

        final BlockProcessorContext blockProcessorContext = new BlockProcessorContext(blockInflaters, transactionInflaters, blockStore, _fullNodeDatabaseManagerFactory, new MutableNetworkTime(), _synchronizationStatus, _transactionValidatorFactory);
        final PendingBlockLoaderContext pendingBlockLoaderContext = new PendingBlockLoaderContext(blockInflaters, _fullNodeDatabaseManagerFactory, _threadPool);
        final BlockchainBuilderContext blockchainBuilderContext = new BlockchainBuilderContext(blockInflaters, _fullNodeDatabaseManagerFactory, bitcoinNodeManager, _threadPool);

        final BlockProcessor blockProcessor = new BlockProcessor(blockProcessorContext);
        final PendingBlockLoader pendingBlockLoader = new PendingBlockLoader(pendingBlockLoaderContext, 1);

        try (final FullNodeDatabaseManager databaseManager = _fullNodeDatabaseManagerFactory.newDatabaseManager()) {
            final FullNodePendingBlockDatabaseManager pendingBlockDatabaseManager = databaseManager.getPendingBlockDatabaseManager();

            for (final String blockData : new String[]{ BlockData.MainChain.GENESIS_BLOCK, BlockData.MainChain.BLOCK_1, BlockData.MainChain.BLOCK_2 }) {
                final Block block = blockInflater.fromBytes(HexUtil.hexStringToByteArray(blockData));
                pendingBlockDatabaseManager.storeBlock(block);
            }

            // Store Unconfirmed transactions...
            // final PendingTransactionDatabaseManager pendingTransactionDatabaseManager = databaseManager.getPendingTransactionDatabaseManager();
            // pendingTransactionDatabaseManager.storeTransaction(signedTransaction0);
        }

        try (final FullNodeDatabaseManager databaseManager = _fullNodeDatabaseManagerFactory.newDatabaseManager()) {
            final TransactionAccumulator transactionAccumulator = SlpTransactionProcessor.createTransactionAccumulator(databaseManager, null);

            { // Store the prerequisite blocks which contains the transaction to lookup...
                final BlockchainBuilder blockchainBuilder = new BlockchainBuilder(blockchainBuilderContext, blockProcessor, pendingBlockLoader, BlockchainBuilderTests.FAKE_DOWNLOAD_STATUS_MONITOR, BlockchainBuilderTests.FAKE_BLOCK_DOWNLOAD_REQUESTER);
                final BlockchainBuilder.StatusMonitor statusMonitor = blockchainBuilder.getStatusMonitor();
                blockchainBuilder.start();
                final int maxSleepCount = 10;
                int sleepCount = 0;
                do {
                    Thread.sleep(250L);
                    sleepCount += 1;

                    if (sleepCount >= maxSleepCount) { throw new RuntimeException("Test execution timeout exceeded."); }
                } while (statusMonitor.getStatus() != SleepyService.Status.SLEEPING);
                blockchainBuilder.stop();
            }

            final Sha256Hash transactionHash0 = Sha256Hash.fromHexString("0E3E2357E806B6CDB1F70B54C3A3A17B6714EE1F0E68BEBB44A74B1EFD512098");
            final Sha256Hash transactionHash1 = Sha256Hash.fromHexString("9B0FC92260312CE44E74EF369F5C66BBB85848F2EDDD5A7A1CDE251E54CCFDD5");
            final List<Sha256Hash> transactionHashes = new ImmutableList<Sha256Hash>(
                transactionHash0,
                transactionHash1
            );

            // Action
            final Map<Sha256Hash, Transaction> transactions = transactionAccumulator.getTransactions(transactionHashes, false);

            // Assert
            final Transaction transaction0 = transactions.get(transactionHash0);
            final Transaction transaction1 = transactions.get(transactionHash1);
            Assert.assertNotNull(transaction0);
            Assert.assertNotNull(transaction1);
            Assert.assertEquals(transactionHash0, transaction0.getHash());
            Assert.assertEquals(transactionHash1, transaction1.getHash());
        }
    }
}
