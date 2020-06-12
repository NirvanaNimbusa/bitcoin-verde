package com.softwareverde.bitcoin.server.module.node.database.block.header.fullnode;

import com.softwareverde.bitcoin.block.BlockId;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.MutableBlockHeader;
import com.softwareverde.bitcoin.block.header.difficulty.Difficulty;
import com.softwareverde.bitcoin.block.header.difficulty.work.BlockWork;
import com.softwareverde.bitcoin.block.header.difficulty.work.ChainWork;
import com.softwareverde.bitcoin.block.header.difficulty.work.MutableChainWork;
import com.softwareverde.bitcoin.chain.segment.BlockchainSegmentId;
import com.softwareverde.bitcoin.chain.time.MedianBlockTimeWithBlocks;
import com.softwareverde.bitcoin.chain.time.MutableMedianBlockTime;
import com.softwareverde.bitcoin.merkleroot.MerkleRoot;
import com.softwareverde.bitcoin.merkleroot.MutableMerkleRoot;
import com.softwareverde.bitcoin.server.database.BatchRunner;
import com.softwareverde.bitcoin.server.database.DatabaseConnection;
import com.softwareverde.bitcoin.server.database.query.BatchedInsertQuery;
import com.softwareverde.bitcoin.server.database.query.Query;
import com.softwareverde.bitcoin.server.database.query.ValueExtractor;
import com.softwareverde.bitcoin.server.module.node.database.DatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.block.BlockRelationship;
import com.softwareverde.bitcoin.server.module.node.database.block.header.BlockHeaderDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.blockchain.BlockchainDatabaseManager;
import com.softwareverde.constable.bytearray.MutableByteArray;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.immutable.ImmutableList;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.database.row.Row;
import com.softwareverde.logging.Logger;
import com.softwareverde.security.hash.sha256.Sha256Hash;
import com.softwareverde.util.Container;
import com.softwareverde.util.Util;

import java.util.HashMap;
import java.util.Map;

public class FullNodeBlockHeaderDatabaseManager implements BlockHeaderDatabaseManager {

    /**
     * Initializes a MedianBlockTime from the database.
     *  NOTE: The headBlockHash is included within the MedianBlockTime.
     */
    protected static MutableMedianBlockTime _newInitializedMedianBlockTime(final BlockHeaderDatabaseManager blockDatabaseManager, final Sha256Hash headBlockHash) throws DatabaseException {
        // Initializes medianBlockTime with the N most recent blocks...

        final MutableMedianBlockTime medianBlockTime = new MutableMedianBlockTime();

        final MutableList<BlockHeader> blockHeadersInDescendingOrder = new MutableList<BlockHeader>(MedianBlockTimeWithBlocks.BLOCK_COUNT);

        Sha256Hash blockHash = headBlockHash;
        for (int i = 0; i < MedianBlockTimeWithBlocks.BLOCK_COUNT; ++i) {
            final BlockId blockId = blockDatabaseManager.getBlockHeaderId(blockHash);
            if (blockId == null) { break; }

            final BlockHeader blockHeader = blockDatabaseManager.getBlockHeader(blockId);
            blockHeadersInDescendingOrder.add(blockHeader);
            blockHash = blockHeader.getPreviousBlockHash();
        }

        // Add the blocks to the MedianBlockTime in ascending order (lowest block-height is added first)...
        final int blockHeaderCount = blockHeadersInDescendingOrder.getCount();
        for (int i = 0; i < blockHeaderCount; ++i) {
            final BlockHeader blockHeader = blockHeadersInDescendingOrder.get(blockHeaderCount - i - 1);
            medianBlockTime.addBlock(blockHeader);
        }

        return medianBlockTime;
    }

    protected final DatabaseManager _databaseManager;

    public FullNodeBlockHeaderDatabaseManager(final DatabaseManager databaseManager) {
        _databaseManager = databaseManager;
    }

    protected Long _getBlockHeight(final BlockId blockId) throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();

        final java.util.List<Row> rows = databaseConnection.query(
            new Query("SELECT id, block_height FROM blocks WHERE id = ?")
                .setParameter(blockId)
        );

        if (rows.isEmpty()) { return null; }

        final Row row = rows.get(0);
        return row.getLong("block_height");
    }

    protected Long _getBlockTimestamp(final BlockId blockId) throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();

        final java.util.List<Row> rows = databaseConnection.query(
            new Query("SELECT id, timestamp FROM blocks WHERE id = ?")
                .setParameter(blockId)
        );

        if (rows.isEmpty()) { return null; }

        final Row row = rows.get(0);
        return row.getLong("timestamp");
    }

    protected BlockId _getBlockHeaderId(final Sha256Hash blockHash) throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();

        final java.util.List<Row> rows = databaseConnection.query(
            new Query("SELECT id FROM blocks WHERE hash = ?")
                .setParameter(blockHash)
        );

        if (rows.isEmpty()) { return null; }

        final Row row = rows.get(0);
        return BlockId.wrap(row.getLong("id"));
    }

    protected Sha256Hash _getBlockHash(final BlockId blockId) throws DatabaseException {
        if (blockId == null) { return null; }

        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();
        final java.util.List<Row> rows = databaseConnection.query(
            new Query("SELECT id, hash FROM blocks WHERE id = ?")
                .setParameter(blockId)
        );

        if (rows.isEmpty()) { return null; }

        final Row row = rows.get(0);
        return Sha256Hash.wrap(row.getBytes("hash"));
    }

    protected BlockHeader _inflateBlockHeader(final BlockId blockId) throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();

        final java.util.List<Row> rows = databaseConnection.query(
            new Query("SELECT * FROM blocks WHERE id = ?")
                .setParameter(blockId)
        );

        if (rows.isEmpty()) { return null; }
        final Row row = rows.get(0);

        final Long version = row.getLong("version");

        final Sha256Hash previousBlockHash;
        {
            final BlockId previousBlockId = BlockId.wrap(row.getLong("previous_block_id"));
            final Sha256Hash nullablePreviousBlockHash = _getBlockHash(previousBlockId);
            previousBlockHash = Util.coalesce(nullablePreviousBlockHash, Sha256Hash.EMPTY_HASH);
        }

        final MerkleRoot merkleRoot = MutableMerkleRoot.copyOf(row.getBytes("merkle_root"));
        final Long timestamp = row.getLong("timestamp");
        final Difficulty difficulty = Difficulty.decode(MutableByteArray.wrap(row.getBytes("difficulty")));
        final Long nonce = row.getLong("nonce");

        final MutableBlockHeader blockHeader = new MutableBlockHeader();

        blockHeader.setVersion(version);
        blockHeader.setPreviousBlockHash(previousBlockHash);
        blockHeader.setMerkleRoot(merkleRoot);
        blockHeader.setTimestamp(timestamp);
        blockHeader.setDifficulty(difficulty);
        blockHeader.setNonce(nonce);

        { // Assert that the hashes match after inflation...
            final Sha256Hash expectedHash = Sha256Hash.wrap(row.getBytes("hash"));
            final Sha256Hash actualHash = blockHeader.getHash();
            if (! Util.areEqual(expectedHash, actualHash)) {
                Logger.warn("Unable to inflate block: " + expectedHash + " / " + blockHeader.getHash());
                return null;
            }
        }

        return blockHeader;
    }

    protected void _updateBlockHeader(final BlockId blockId, final BlockHeader blockHeader) throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();

        final BlockId previousBlockId = _getBlockHeaderId(blockHeader.getPreviousBlockHash());
        final Long previousBlockHeight = _getBlockHeight(previousBlockId);
        final Long blockHeight = (previousBlockHeight == null ? 0 : (previousBlockHeight + 1));

        databaseConnection.executeSql(
            new Query("UPDATE blocks SET hash = ?, previous_block_id = ?, block_height = ?, merkle_root = ?, version = ?, timestamp = ?, difficulty = ?, nonce = ? WHERE id = ?")
                .setParameter(blockHeader.getHash())
                .setParameter(previousBlockId)
                .setParameter(blockHeight)
                .setParameter(blockHeader.getMerkleRoot())
                .setParameter(blockHeader.getVersion())
                .setParameter(blockHeader.getTimestamp())
                .setParameter(blockHeader.getDifficulty())
                .setParameter(blockHeader.getNonce())
                .setParameter(blockId)
        );
    }

    protected ChainWork _getChainWork(final BlockId blockId) throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();

        final java.util.List<Row> rows = databaseConnection.query(
            new Query("SELECT id, chain_work FROM blocks WHERE id = ?")
                .setParameter(blockId)
        );

        if (rows.isEmpty()) { return null; }

        final Row row = rows.get(0);
        return ChainWork.wrap(row.getBytes("chain_work"));
    }

    protected BlockId _insertBlockHeader(final BlockHeader blockHeader) throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();

        final BlockId previousBlockId = _getBlockHeaderId(blockHeader.getPreviousBlockHash());
        final Long previousBlockHeight = _getBlockHeight(previousBlockId);
        final Long blockHeight = (previousBlockId == null ? 0 : (previousBlockHeight + 1));
        final Difficulty difficulty = blockHeader.getDifficulty();

        final BlockWork blockWork = difficulty.calculateWork();
        final ChainWork previousChainWork = (previousBlockId == null ? new MutableChainWork() : _getChainWork(previousBlockId));
        final ChainWork chainWork = ChainWork.add(previousChainWork, blockWork);

        return BlockId.wrap(databaseConnection.executeSql(
            new Query("INSERT INTO blocks (hash, previous_block_id, block_height, merkle_root, version, timestamp, difficulty, nonce, chain_work) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
                .setParameter(blockHeader.getHash())
                .setParameter(previousBlockId)
                .setParameter(blockHeight)
                .setParameter(blockHeader.getMerkleRoot())
                .setParameter(blockHeader.getVersion())
                .setParameter(blockHeader.getTimestamp())
                .setParameter(difficulty)
                .setParameter(blockHeader.getNonce())
                .setParameter(chainWork)
        ));
    }

    protected List<BlockId> _insertBlockHeaders(final List<BlockHeader> blockHeaders, final Integer maxBatchSize) throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();

        if (blockHeaders.isEmpty()) {
            return new MutableList<BlockId>(0);
        }

        if (blockHeaders.getCount() == 1) {
            final BlockHeader blockHeader = blockHeaders.get(0);
            final BlockId blockId = _insertBlockHeader(blockHeader);
            return new ImmutableList<BlockId>(blockId);
        }

        final MutableList<BlockId> blockIds = new MutableList<BlockId>(blockHeaders.getCount());

        final Container<Long> previousBlockHeight = new Container<Long>();
        final Container<ChainWork> previousChainWork = new Container<ChainWork>();
        final Container<BlockId> lastInsertedBlockId = new Container<BlockId>();

        final BatchRunner<BlockHeader> batchRunner = new BatchRunner<BlockHeader>(maxBatchSize);
        batchRunner.run(blockHeaders, new BatchRunner.Batch<BlockHeader>() {
            @Override
            public void run(final List<BlockHeader> batchedBlockHeaders) throws Exception {
                final int batchCount = batchedBlockHeaders.getCount();
                int i = 0;

                // Since the next insert_id of the blocks table may not be the previous blockId + 1, insert the first blockHeader and retrieve its auto_increment value, then proceed with regular batching...
                if (lastInsertedBlockId.value == null) {
                    final BlockHeader blockHeader = batchedBlockHeaders.get(0);
                    final BlockId blockId = _insertBlockHeader(blockHeader);

                    lastInsertedBlockId.value = blockId;
                    previousBlockHeight.value = _getBlockHeight(blockId);
                    previousChainWork.value = _getChainWork(blockId);
                    blockIds.add(blockId);

                    i += 1;
                }

                final BatchedInsertQuery batchedInsertQuery = new BatchedInsertQuery("INSERT INTO blocks (hash, previous_block_id, block_height, merkle_root, version, timestamp, difficulty, nonce, chain_work) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)");

                long previousBlockId = lastInsertedBlockId.value.longValue();
                while (i < batchCount) {
                    final BlockHeader blockHeader = batchedBlockHeaders.get(i);

                    long blockHeight = (previousBlockHeight.value + 1L);
                    final Difficulty difficulty = blockHeader.getDifficulty();

                    final BlockWork blockWork = difficulty.calculateWork();
                    final ChainWork chainWork = ChainWork.add(previousChainWork.value, blockWork);

                    batchedInsertQuery.setParameter(blockHeader.getHash());
                    batchedInsertQuery.setParameter(previousBlockId);
                    batchedInsertQuery.setParameter(blockHeight);
                    batchedInsertQuery.setParameter(blockHeader.getMerkleRoot());
                    batchedInsertQuery.setParameter(blockHeader.getVersion());
                    batchedInsertQuery.setParameter(blockHeader.getTimestamp());
                    batchedInsertQuery.setParameter(difficulty);
                    batchedInsertQuery.setParameter(blockHeader.getNonce());
                    batchedInsertQuery.setParameter(chainWork);

                    previousBlockId += 1L;
                    previousBlockHeight.value = blockHeight;
                    previousChainWork.value = chainWork;

                    blockIds.add(BlockId.wrap(lastInsertedBlockId.value.longValue() + i));

                    i += 1;
                }

                lastInsertedBlockId.value = BlockId.wrap(databaseConnection.executeSql(batchedInsertQuery));
            }
        });

        return blockIds;
    }

    protected void _setBlockchainSegmentId(final BlockId blockId, final BlockchainSegmentId blockchainSegmentId) throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();

        databaseConnection.executeSql(
            new Query("UPDATE blocks SET blockchain_segment_id = ? WHERE id = ?")
                .setParameter(blockchainSegmentId)
                .setParameter(blockId)
        );
    }

    protected void _setBlockchainSegmentIds(final List<BlockId> blockIds, final BlockchainSegmentId blockchainSegmentId) throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();

        databaseConnection.executeSql(
            new Query("UPDATE blocks SET blockchain_segment_id = ? WHERE id IN (?)")
                .setParameter(blockchainSegmentId)
                .setInClauseParameters(blockIds, ValueExtractor.IDENTIFIER)
        );
    }

    protected BlockchainSegmentId _getBlockchainSegmentId(final BlockId blockId) throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();

        final java.util.List<Row> rows = databaseConnection.query(
            new Query("SELECT id, blockchain_segment_id FROM blocks WHERE id = ?")
                .setParameter(blockId)
        );
        if (rows.isEmpty()) { return null; }
        final Row row = rows.get(0);

        return BlockchainSegmentId.wrap(row.getLong("blockchain_segment_id"));
    }

    protected BlockId _getBlockIdAtBlockHeight(final BlockchainSegmentId blockchainSegmentId, final Long blockHeight) throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();

        final java.util.List<Row> rows = databaseConnection.query(
            new Query("SELECT id FROM blocks WHERE blockchain_segment_id = ? AND block_height = ?")
                .setParameter(blockchainSegmentId)
                .setParameter(blockHeight)
        );

        if (rows.isEmpty()) { return null; }

        final Row row = rows.get(0);
        return BlockId.wrap(row.getLong("id"));
    }

    protected Boolean _isBlockConnectedToChain(final BlockId blockId, final BlockchainSegmentId blockchainSegmentId, final BlockRelationship blockRelationship) throws DatabaseException {
        final BlockchainDatabaseManager blockchainDatabaseManager = _databaseManager.getBlockchainDatabaseManager();

        final BlockchainSegmentId blockchainSegmentId1 = _getBlockchainSegmentId(blockId);
        return blockchainDatabaseManager.areBlockchainSegmentsConnected(blockchainSegmentId1, blockchainSegmentId, blockRelationship);
    }

    protected BlockId _getChildBlockId(final BlockchainSegmentId blockchainSegmentId, final BlockId previousBlockId) throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();
        final BlockchainDatabaseManager blockchainDatabaseManager = _databaseManager.getBlockchainDatabaseManager();

        final java.util.List<Row> rows = databaseConnection.query(
            new Query("SELECT id, blockchain_segment_id FROM blocks WHERE previous_block_id = ?")
                .setParameter(previousBlockId)
        );

        if (rows.isEmpty()) { return null; }

        if (rows.size() == 1) {
            final Row row = rows.get(0);
            return BlockId.wrap(row.getLong("id"));
        }

        // At this point, previousBlockId has multiple children.
        // If blockchainSegmentId is not provided, then just return the first-seen block.
        if (blockchainSegmentId == null) {
            final Row row = rows.get(0);
            return BlockId.wrap(row.getLong("id"));
        }

        // Since blockchainSegmentId is provided, the child along its chain is the blockId that shall be preferred...
        for (final Row row : rows) {
            final BlockId blockId = BlockId.wrap(row.getLong("id"));
            final BlockchainSegmentId blockchainSegmentId1 = BlockchainSegmentId.wrap(row.getLong("blockchain_segment_id"));
            final Boolean blockIsConnectedToChain = blockchainDatabaseManager.areBlockchainSegmentsConnected(blockchainSegmentId1, blockchainSegmentId, BlockRelationship.ANCESTOR);
            if (blockIsConnectedToChain) {
                return blockId;
            }
        }

        // None of the children blocks match the blockchainSegmentId, so null is returned...
        return null;
    }

    protected Sha256Hash _getHeadBlockHeaderHash() throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();

        final java.util.List<Row> rows = databaseConnection.query(
            new Query("SELECT id, hash FROM head_block_header")
        );
        if (rows.isEmpty()) { return null; }

        final Row row = rows.get(0);
        return Sha256Hash.copyOf(row.getBytes("hash"));
    }

    protected BlockId _getHeadBlockHeaderId() throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();

        final java.util.List<Row> rows = databaseConnection.query(
            new Query("SELECT id, hash FROM head_block_header")
        );
        if (rows.isEmpty()) { return null; }

        final Row row = rows.get(0);
        return BlockId.wrap(row.getLong("id"));
    }

    protected Integer _getBlockHeaderDirectDescendantCount(final BlockId blockId) throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();

        final java.util.List<Row> rows = databaseConnection.query(
            new Query("SELECT id FROM blocks WHERE previous_block_id = ?")
                .setParameter(blockId)
        );

        return (rows.size());
    }

    protected BlockId _getPreviousBlockId(final BlockId blockId) throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();

        final java.util.List<Row> rows = databaseConnection.query(
            new Query("SELECT id, previous_block_id FROM blocks WHERE id = ?")
                .setParameter(blockId)
        );
        if (rows.isEmpty()) { return null; }

        final Row row = rows.get(0);
        return BlockId.wrap(row.getLong("previous_block_id"));
    }

    @Override
    public BlockId insertBlockHeader(final BlockHeader blockHeader) throws DatabaseException {
        if (! Thread.holdsLock(MUTEX)) { throw new RuntimeException("Attempting to insertBlockHeader without obtaining lock."); }

        final BlockchainDatabaseManager blockchainDatabaseManager = _databaseManager.getBlockchainDatabaseManager();

        final BlockId blockId = _insertBlockHeader(blockHeader);

        blockchainDatabaseManager.updateBlockchainsForNewBlock(blockId);

        return blockId;
    }

    @Override
    public void updateBlockHeader(final BlockId blockId, final BlockHeader blockHeader) throws DatabaseException {
        _updateBlockHeader(blockId, blockHeader);
    }

    @Override
    public BlockId storeBlockHeader(final BlockHeader blockHeader) throws DatabaseException {
        if (! Thread.holdsLock(MUTEX)) { throw new RuntimeException("Attempting to storeBlockHeader without obtaining lock."); }

        final BlockchainDatabaseManager blockchainDatabaseManager = _databaseManager.getBlockchainDatabaseManager();

        final BlockId existingBlockId = _getBlockHeaderId(blockHeader.getHash());

        if (existingBlockId != null) {
            return existingBlockId;
        }

        final BlockId blockId = _insertBlockHeader(blockHeader);

        blockchainDatabaseManager.updateBlockchainsForNewBlock(blockId);

        return blockId;
    }

    /**
     * Batch-Inserts the provided BlockHeaders.  The BlockHeaders must be provided in-order relative to one another in
     *  ascending order; aka, each BlockHeader must be the (only) child of the previous BlockHeader.
     * This function is intended to be used for bootstrapping a database with a known set of headers; be extra careful
     *  when using this function in other circumstances.
     * Each BlockHeader is inserted with the next assumed block height.  The first BlockHeader's height is calculated
     *  by attempting to look up its previous block via its PreviousBlockHash.  If it is not found, it is assumed to be
     *  a genesis block and is inserted at height 0.
     * This function is safe to invoke if other BlockHeaders have been stored, if, and only if, each header is guaranteed
     *  to not be contentious with another block.
     * The BlockchainSegmentId assigned to every BlockHeader (except the first) is the same as its parent.  The first
     *  BlockHeader's BlockchainSegmentId is assigned by the normal BlockchainDatabaseManager::updateBlockchainsForNewBlock
     *  method.
     */
    @Override
    public List<BlockId> insertBlockHeaders(final List<BlockHeader> blockHeaders) throws DatabaseException {
        if (! Thread.holdsLock(MUTEX)) { throw new RuntimeException("Attempting to storeBlockHeader without obtaining lock."); }
        if (blockHeaders.isEmpty()) { return new MutableList<BlockId>(0); }

        final BlockchainDatabaseManager blockchainDatabaseManager = _databaseManager.getBlockchainDatabaseManager();

        final List<BlockId> blockIds = _insertBlockHeaders(blockHeaders, Integer.MAX_VALUE);

        final BlockchainSegmentId blockchainSegmentId = blockchainDatabaseManager.updateBlockchainsForNewBlock(blockIds.get(0));

        _setBlockchainSegmentIds(blockIds, blockchainSegmentId);

        return blockIds;
    }

    @Override
    public List<BlockId> insertBlockHeaders(final List<BlockHeader> blockHeaders, final Integer maxBatchSize) throws DatabaseException {
        if (! Thread.holdsLock(MUTEX)) { throw new RuntimeException("Attempting to storeBlockHeader without obtaining lock."); }
        if (blockHeaders.isEmpty()) { return new MutableList<BlockId>(0); }

        final BlockchainDatabaseManager blockchainDatabaseManager = _databaseManager.getBlockchainDatabaseManager();

        final List<BlockId> blockIds = _insertBlockHeaders(blockHeaders, maxBatchSize);

        final BlockchainSegmentId blockchainSegmentId = blockchainDatabaseManager.updateBlockchainsForNewBlock(blockIds.get(0));

        _setBlockchainSegmentIds(blockIds, blockchainSegmentId);

        return blockIds;
    }

    @Override
    public void setBlockByteCount(final BlockId blockId, final Integer byteCount) throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();

        databaseConnection.executeSql(
            new Query("UPDATE blocks SET byte_count = ? WHERE id = ?")
                .setParameter(byteCount)
                .setParameter(blockId)
        );
    }

    @Override
    public Integer getBlockByteCount(final BlockId blockId) throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();

        final java.util.List<Row> rows = databaseConnection.query(
            new Query("SELECT id, byte_count FROM blocks WHERE id = ?")
                .setParameter(blockId)
        );
        if (rows.isEmpty()) { return null; }

        final Row row = rows.get(0);
        return row.getInteger("byte_count");
    }

    /**
     * Returns the Sha256Hash of the block that has the tallest block-height.
     */
    @Override
    public Sha256Hash getHeadBlockHeaderHash() throws DatabaseException {
        return _getHeadBlockHeaderHash();
    }

    /**
     * Returns the BlockId of the block that has the tallest block-height.
     */
    @Override
    public BlockId getHeadBlockHeaderId() throws DatabaseException {
        return _getHeadBlockHeaderId();
    }

    @Override
    public BlockId getBlockHeaderId(final Sha256Hash blockHash) throws DatabaseException {
        return _getBlockHeaderId(blockHash);
    }

    @Override
    public BlockHeader getBlockHeader(final BlockId blockId) throws DatabaseException {
        return _inflateBlockHeader(blockId);
    }

    /**
     * Returns true if the BlockHeader has been downloaded and verified.
     */
    @Override
    public Boolean blockHeaderExists(final Sha256Hash blockHash) throws DatabaseException {
        final BlockId blockId = _getBlockHeaderId(blockHash);
        return (blockId != null);
    }

    @Override
    public Integer getBlockDirectDescendantCount(final BlockId blockId) throws DatabaseException {
        return _getBlockHeaderDirectDescendantCount(blockId);
    }

    @Override
    public void setBlockchainSegmentId(final BlockId blockId, final BlockchainSegmentId blockchainSegmentId) throws DatabaseException {
        _setBlockchainSegmentId(blockId, blockchainSegmentId);
    }

    @Override
    public BlockchainSegmentId getBlockchainSegmentId(final BlockId blockId) throws DatabaseException {
        return _getBlockchainSegmentId(blockId);
    }

    @Override
    public Long getBlockHeight(final BlockId blockId) throws DatabaseException {
        return _getBlockHeight(blockId);
    }

    @Override
    public Map<BlockId, Long> getBlockHeights(final List<BlockId> blockIds) throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();

        final HashMap<BlockId, Long> blockHeights = new HashMap<BlockId, Long>(blockIds.getCount());
        final BatchRunner<BlockId> batchRunner = new BatchRunner<BlockId>(1024);
        batchRunner.run(blockIds, new BatchRunner.Batch<BlockId>() {
            @Override
            public void run(final List<BlockId> blockIds) throws Exception {
                final java.util.List<Row> rows = databaseConnection.query(
                    new Query("SELECT id, block_height FROM blocks WHERE id IN (?)")
                        .setInClauseParameters(blockIds, ValueExtractor.IDENTIFIER)
                );

                for (final Row row : rows) {
                    final BlockId blockId = BlockId.wrap(row.getLong("id"));
                    final Long blockHeight = row.getLong("block_height");
                    blockHeights.put(blockId, blockHeight);
                }
            }
        });
        return blockHeights;
    }

    @Override
    public Long getBlockTimestamp(final BlockId blockId) throws DatabaseException {
        return _getBlockTimestamp(blockId);
    }

    @Override
    public BlockId getChildBlockId(final BlockchainSegmentId blockchainSegmentId, final BlockId previousBlockId) throws DatabaseException {
        return _getChildBlockId(blockchainSegmentId, previousBlockId);
    }

    @Override
    public Boolean hasChildBlock(final BlockId blockId) throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();
        java.util.List<Row> rows = databaseConnection.query(
            new Query("SELECT id FROM blocks WHERE previous_block_id = ? LIMIT 1")
                .setParameter(blockId)
        );
        return (! rows.isEmpty());
    }

    /**
     *
     *     E         E'
     *     |         |
     *  #4 +----D----+ #5           Height: 3
     *          |
     *          C         C''       Height: 2
     *          |         |
     *       #2 +----B----+ #3      Height: 1
     *               |
     *               A #1           Height: 0
     *
     * Block C is an ancestor of Chain #4.
     * Block E is a descendant of Chain #1.
     *
     */
    @Override
    public Boolean isBlockConnectedToChain(final BlockId blockId, final BlockchainSegmentId blockchainSegmentId, final BlockRelationship blockRelationship) throws DatabaseException {
        return _isBlockConnectedToChain(blockId, blockchainSegmentId, blockRelationship);
    }

    @Override
    public Sha256Hash getBlockHash(final BlockId blockId) throws DatabaseException {
        return _getBlockHash(blockId);
    }

    /**
     * Returns a list of Block hashes for the provided list of BlockIds.
     *  The order of the Block hashes corresponds to order within blockIds.
     *  If a Block hash could not be found then the item is set to null within the returned list.
     *  blockIds does not need to be sorted, and may be in any order.
     */
    @Override
    public List<Sha256Hash> getBlockHashes(final List<BlockId> blockIds) throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();
        final java.util.List<Row> rows = databaseConnection.query(
            new Query("SELECT id, hash FROM blocks WHERE id IN (?)")
                .setInClauseParameters(blockIds, ValueExtractor.IDENTIFIER)
        );

        final HashMap<BlockId, Sha256Hash> hashesMap = new HashMap<BlockId, Sha256Hash>(rows.size());
        for (final Row row : rows) {
            final BlockId blockId = BlockId.wrap(row.getLong("id"));
            final Sha256Hash blockHash = Sha256Hash.copyOf(row.getBytes("hash"));

            hashesMap.put(blockId, blockHash);
        }

        final MutableList<Sha256Hash> blockHashes = new MutableList<Sha256Hash>(blockIds.getCount());
        for (final BlockId blockId : blockIds) {
            blockHashes.add(hashesMap.get(blockId));
        }
        return blockHashes;
    }

    /**
     * Returns the BlockId of the nth-parent, where n is the parentCount.
     *  For instance, getAncestor(blockId, 0) returns blockId, and getAncestor(blockId, 1) returns blockId's parent.
     */
    @Override
    public BlockId getAncestorBlockId(final BlockId blockId, final Integer parentCount) throws DatabaseException {
        if (blockId == null) { return null; }

        if (parentCount == 1) {
            // Optimization/Specialization for parentBlockId...
            return _getPreviousBlockId(blockId);
        }

        BlockId nextBlockId = blockId;
        for (int i = 0; i < parentCount; ++i) {
            final BlockHeader blockHeader = _inflateBlockHeader(nextBlockId);
            if (blockHeader == null) { return null; }

            nextBlockId = _getBlockHeaderId(blockHeader.getPreviousBlockHash());
        }
        return nextBlockId;
    }

    /**
     * Initializes a Mutable MedianBlockTime using only blocks that have been fully validated.
     */
    @Override
    public MutableMedianBlockTime initializeMedianBlockTime() throws DatabaseException {
        Sha256Hash blockHash = Util.coalesce(_getHeadBlockHeaderHash(), BlockHeader.GENESIS_BLOCK_HASH);
        return _newInitializedMedianBlockTime(this, blockHash);
    }

    /**
     * Initializes a Mutable MedianBlockTime using most recent block headers.
     *  The significant difference between MutableMedianBlockTime.newInitializedMedianBlockHeaderTime and MutableMedianBlockTime.newInitializedMedianBlockTime
     *  is that BlockHeaders are downloaded and validated more quickly than blocks; therefore when validating blocks
     *  MutableMedianBlockTime.newInitializedMedianBlockTime should be used, not this function.
     */
    @Override
    public MutableMedianBlockTime initializeMedianBlockHeaderTime() throws DatabaseException {
        final Sha256Hash headBlockHash = _getHeadBlockHeaderHash();
        Sha256Hash blockHash = Util.coalesce(headBlockHash, BlockHeader.GENESIS_BLOCK_HASH);
        return _newInitializedMedianBlockTime(this, blockHash);
    }

    /**
     * Calculates the MedianBlockTime of the provided blockId.
     * NOTE: startingBlockId is exclusive. The MedianBlockTime does NOT include the provided startingBlockId; instead,
     *  it includes the MedianBlockTime.BLOCK_COUNT (11) number of blocks before the startingBlockId.
     */
    @Override
    public MutableMedianBlockTime calculateMedianBlockTimeBefore(final BlockId blockId) throws DatabaseException {
        final BlockId previousBlockId = _getPreviousBlockId(blockId);
        if (previousBlockId == null) { return null; }
        final Sha256Hash blockHash = _getBlockHash(previousBlockId);
        return _newInitializedMedianBlockTime(this, blockHash);
    }

    /**
     * Calculates the MedianBlockTime of the provided blockId.
     * NOTE: This method is identical to BlockHeaderDatabaseManager::calculateMedianBlockTime except that blockId is inclusive.
     */
    @Override
    public MutableMedianBlockTime calculateMedianBlockTime(final BlockId blockId) throws DatabaseException {
        final Sha256Hash blockHash = _getBlockHash(blockId);
        return _newInitializedMedianBlockTime(this, blockHash);
    }

    @Override
    public ChainWork getChainWork(final BlockId blockId) throws DatabaseException {
        return _getChainWork(blockId);
    }

    @Override
    public BlockId getBlockIdAtHeight(final BlockchainSegmentId blockchainSegmentId, final Long blockHeight) throws DatabaseException {
        final DatabaseConnection databaseConnection = _databaseManager.getDatabaseConnection();

        final java.util.List<Row> rows = databaseConnection.query(
            new Query("SELECT id FROM blocks WHERE block_height = ?")
                .setParameter(blockHeight)
        );

        for (final Row row : rows) {
            final BlockId blockId = BlockId.wrap(row.getLong("id"));

            if (blockchainSegmentId == null) {
                return blockId;
            }

            final Boolean blockIsConnectedToChain = _isBlockConnectedToChain(blockId, blockchainSegmentId, BlockRelationship.ANY);
            if (blockIsConnectedToChain) {
                return blockId;
            }
        }

        return null;
    }
}
