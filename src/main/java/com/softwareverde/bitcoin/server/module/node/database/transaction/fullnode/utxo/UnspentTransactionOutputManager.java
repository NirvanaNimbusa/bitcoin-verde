package com.softwareverde.bitcoin.server.module.node.database.transaction.fullnode.utxo;

import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockId;
import com.softwareverde.bitcoin.server.database.DatabaseConnection;
import com.softwareverde.bitcoin.server.database.DatabaseConnectionFactory;
import com.softwareverde.bitcoin.server.database.query.Query;
import com.softwareverde.bitcoin.server.module.node.database.block.fullnode.FullNodeBlockDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.block.header.BlockHeaderDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.fullnode.FullNodeDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.transaction.fullnode.FullNodeTransactionDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.transaction.fullnode.UtxoDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.sync.blockloader.BlockLoader;
import com.softwareverde.bitcoin.server.module.node.sync.blockloader.PreloadedBlock;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.input.TransactionInput;
import com.softwareverde.bitcoin.transaction.output.identifier.TransactionOutputIdentifier;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.Util;
import com.softwareverde.util.timer.MilliTimer;

public class UnspentTransactionOutputManager {
    protected final DatabaseConnectionFactory _databaseConnectionFactory;
    protected final FullNodeDatabaseManager _databaseManager;

    public void _buildUtxoSet(final BlockLoader blockLoader) throws DatabaseException {
        final BlockHeaderDatabaseManager blockHeaderDatabaseManager = _databaseManager.getBlockHeaderDatabaseManager();
        final FullNodeBlockDatabaseManager blockDatabaseManager = _databaseManager.getBlockDatabaseManager();
        final FullNodeTransactionDatabaseManager transactionDatabaseManager = _databaseManager.getTransactionDatabaseManager();

        final BlockId headBlockId = blockDatabaseManager.getHeadBlockId();
        final long maxBlockHeight = Util.coalesce(blockHeaderDatabaseManager.getBlockHeight(headBlockId), 0L);

        long blockHeight = (transactionDatabaseManager.getCommittedUnspentTransactionOutputBlockHeight() + 1L); // inclusive
        while (blockHeight <= maxBlockHeight) {
            final PreloadedBlock preloadedBlock = blockLoader.getBlock(blockHeight);
            if (preloadedBlock == null) {
                Logger.debug("Unable to load block: " + blockHeight);
                return;
            }

            _updateUtxoSetWithBlock(preloadedBlock.getBlock(), preloadedBlock.getBlockHeight());
            blockHeight += 1L;
        }
    }

    protected void _commitInMemoryUtxoSetToDisk() throws DatabaseException {
        final FullNodeTransactionDatabaseManager transactionDatabaseManager = _databaseManager.getTransactionDatabaseManager();
        transactionDatabaseManager.commitUnspentTransactionOutputs(_databaseConnectionFactory);
    }

    protected void _updateUtxoSetWithBlock(final Block block, final Long blockHeight) throws DatabaseException {
        final FullNodeTransactionDatabaseManager transactionDatabaseManager = _databaseManager.getTransactionDatabaseManager();

        final MilliTimer totalTimer = new MilliTimer();
        final MilliTimer utxoCommitTimer = new MilliTimer();
        final MilliTimer utxoTimer = new MilliTimer();

        totalTimer.start();

        final List<Transaction> transactions = block.getTransactions();
        final int transactionCount = transactions.getCount();

        final MutableList<TransactionOutputIdentifier> spentTransactionOutputIdentifiers = new MutableList<TransactionOutputIdentifier>();
        final MutableList<TransactionOutputIdentifier> unspentTransactionOutputIdentifiers = new MutableList<TransactionOutputIdentifier>();
        for (int i = 0; i < transactions.getCount(); ++i) {
            final Transaction transaction = transactions.get(i);

            final boolean isCoinbase = (i == 0);
            if (! isCoinbase) {
                for (final TransactionInput transactionInput : transaction.getTransactionInputs()) {
                    final TransactionOutputIdentifier transactionOutputIdentifier = TransactionOutputIdentifier.fromTransactionInput(transactionInput);
                    spentTransactionOutputIdentifiers.add(transactionOutputIdentifier);
                }
            }

            final List<TransactionOutputIdentifier> transactionOutputIdentifiers = TransactionOutputIdentifier.fromTransactionOutputs(transaction);
            unspentTransactionOutputIdentifiers.addAll(transactionOutputIdentifiers);
        }

        final int worstCaseNewUtxoCount = (unspentTransactionOutputIdentifiers.getCount() + spentTransactionOutputIdentifiers.getCount());
        final Long uncommittedUtxoCount = transactionDatabaseManager.getCachedUnspentTransactionOutputCount();
        if ( ((blockHeight % 2016L) == 0L) || ( (uncommittedUtxoCount + worstCaseNewUtxoCount) >= UtxoDatabaseManager.DEFAULT_MAX_UTXO_CACHE_COUNT) ) {
            utxoCommitTimer.start();
            _commitInMemoryUtxoSetToDisk();
            utxoCommitTimer.stop();
            Logger.debug("Commit Timer: " + utxoCommitTimer.getMillisecondsElapsed() + "ms.");
        }

        utxoTimer.start();
        FullNodeTransactionDatabaseManager.UTXO_WRITE_MUTEX.lock();
        try {
            transactionDatabaseManager.insertUnspentTransactionOutputs(unspentTransactionOutputIdentifiers, blockHeight);
            transactionDatabaseManager.markTransactionOutputsAsSpent(spentTransactionOutputIdentifiers);
        }
        finally {
            FullNodeTransactionDatabaseManager.UTXO_WRITE_MUTEX.unlock();
        }

        utxoTimer.stop();
        totalTimer.stop();

        Logger.debug("BlockHeight: " + blockHeight + " " + unspentTransactionOutputIdentifiers.getCount() + " unspent, " + spentTransactionOutputIdentifiers.getCount() + " spent. " + transactionCount + " in " + totalTimer.getMillisecondsElapsed() + " ms (" + (transactionCount * 1000L / (totalTimer.getMillisecondsElapsed() + 1L)) + " tps) " + utxoTimer.getMillisecondsElapsed() + "ms UTXO " + (transactions.getCount() * 1000L / (utxoTimer.getMillisecondsElapsed() + 1L)) + " tps");
    }

    public UnspentTransactionOutputManager(final FullNodeDatabaseManager databaseManager, final DatabaseConnectionFactory databaseConnectionFactory) {
        _databaseManager = databaseManager;
        _databaseConnectionFactory = databaseConnectionFactory;
    }

    /**
     * Destroys the In-Memory and On-Disk UTXO set and rebuilds it from the Genesis Block.
     */
    public void rebuildUtxoSetFromGenesisBlock(final DatabaseConnection maintenanceDatabaseConnection, final BlockLoader blockLoader) throws DatabaseException {
        FullNodeTransactionDatabaseManager.UTXO_WRITE_MUTEX.lock();
        try {
            maintenanceDatabaseConnection.executeSql(new Query("TRUNCATE TABLE unspent_transaction_outputs"));
            maintenanceDatabaseConnection.executeSql(new Query("TRUNCATE TABLE committed_unspent_transaction_outputs"));

            _buildUtxoSet(blockLoader);
        }
        finally {
            FullNodeTransactionDatabaseManager.UTXO_WRITE_MUTEX.unlock();
        }
    }

    /**
     * Builds the UTXO set from the last committed block.
     */
    public void buildUtxoSet(final BlockLoader blockLoader) throws DatabaseException {
        _buildUtxoSet(blockLoader);
    }

    /**
     * Updates the in-memory UTXO set for the provided Block.
     *  This function may do a disk-commit of the in-memory UTXO set.
     *  A disk-commit is executed periodically based on block height and/or if the in-memory set grows too large.
     */
    public void updateUtxoSetWithBlock(final Block block, final Long blockHeight) throws DatabaseException {
        _updateUtxoSetWithBlock(block, blockHeight);
    }

    /**
     * Commits the in-memory UTXO set to disk.
     * A disk-commit is typically a lengthy process and blocks the UTXO set until complete.
     */
    public void commitInMemoryUtxoSetToDisk() throws DatabaseException {
        _commitInMemoryUtxoSetToDisk();
    }
}
