package com.softwareverde.bitcoin.server.module.node.sync;

import com.softwareverde.bitcoin.block.BlockId;
import com.softwareverde.bitcoin.chain.segment.BlockchainSegmentId;
import com.softwareverde.security.hash.sha256.Sha256Hash;
import com.softwareverde.bitcoin.server.module.node.database.DatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.block.BlockDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.block.header.BlockHeaderDatabaseManager;
import com.softwareverde.bitcoin.util.BitcoinUtil;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.database.DatabaseException;

public class BlockFinderHashesBuilder {
    protected final DatabaseManager _databaseManager;

    public BlockFinderHashesBuilder(final DatabaseManager databaseManager) {
        _databaseManager = databaseManager;
    }

    public List<Sha256Hash> createBlockFinderBlockHashes() throws DatabaseException {
        final BlockHeaderDatabaseManager blockHeaderDatabaseManager = _databaseManager.getBlockHeaderDatabaseManager();
        final BlockDatabaseManager blockDatabaseManager = _databaseManager.getBlockDatabaseManager();

        final Long maxBlockHeight;
        final BlockchainSegmentId headBlockchainSegmentId;
        final BlockId headBlockId = blockDatabaseManager.getHeadBlockId();
        if (headBlockId != null) {
            headBlockchainSegmentId = blockHeaderDatabaseManager.getBlockchainSegmentId(headBlockId);
            maxBlockHeight = blockHeaderDatabaseManager.getBlockHeight(headBlockId);
        }
        else {
            maxBlockHeight = 0L;
            headBlockchainSegmentId = null;
        }

        final MutableList<Sha256Hash> blockHashes = new MutableList<Sha256Hash>(BitcoinUtil.log2(maxBlockHeight.intValue()) + 11);
        int blockHeightStep = 1;
        for (Long blockHeight = maxBlockHeight; blockHeight > 0L; blockHeight -= blockHeightStep) {
            final BlockId blockId = blockHeaderDatabaseManager.getBlockIdAtHeight(headBlockchainSegmentId, blockHeight);
            final Sha256Hash blockHash = blockHeaderDatabaseManager.getBlockHash(blockId);

            blockHashes.add(blockHash);

            if (blockHashes.getCount() >= 10) {
                blockHeightStep *= 2;
            }
        }

        return blockHashes;
    }
}
