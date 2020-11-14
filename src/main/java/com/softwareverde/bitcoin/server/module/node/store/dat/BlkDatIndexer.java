package com.softwareverde.bitcoin.server.module.node.store.dat;

import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.BlockHeaderInflater;
import com.softwareverde.bitcoin.util.ByteUtil;
import com.softwareverde.constable.bytearray.MutableByteArray;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.util.Tuple;
import com.softwareverde.util.Util;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class BlkDatIndexer {
    public interface Callback {
        void onNewBlkDatIndices(List<Tuple<Sha256Hash, BlkDatIndex.FilePointer>> filePointers);
    }

    protected final BlockHeaderInflater _blockHeaderInflater;
    protected final String _blkDatDirectory;
    protected final Callback _callback;

    protected List<Tuple<Sha256Hash, BlkDatIndex.FilePointer>> _readBlkDat(final File blkDatFile) throws Exception {
        final int blockMagicByteCount = BlkDatBlockStore.BLOCK_MAGIC.getByteCount();
        final MutableByteArray blockMagicBuffer = new MutableByteArray(blockMagicByteCount);
        final MutableByteArray blockByteCountBuffer = new MutableByteArray(4);

        final String blkDatFileName = blkDatFile.getName();
        final MutableList<Tuple<Sha256Hash, BlkDatIndex.FilePointer>> filePointers = new MutableList<>();

        try (final FileInputStream fileInputStream = new FileInputStream(blkDatFile)) {
            long fileOffset = 0L;
            while (true) {
                final BlkDatIndex.FilePointer filePointer = new BlkDatIndex.FilePointer(blkDatFileName, fileOffset);
                final int byteCount = fileInputStream.read(blockMagicBuffer.unwrap());
                if (byteCount != blockMagicByteCount) { break; }
                fileOffset += byteCount;

                if (! Util.areEqual(BlkDatBlockStore.BLOCK_MAGIC, blockMagicBuffer)) {
                    throw new Exception("Invalid blkDat " + blkDatFile + ". Expected " + BlkDatBlockStore.BLOCK_MAGIC + ", found " + blockMagicBuffer);
                }

                final int blockSizeByteCount = fileInputStream.read(blockByteCountBuffer.unwrap());
                fileOffset += blockSizeByteCount;
                if (blockSizeByteCount != blockByteCountBuffer.getByteCount()) {
                    throw new Exception("Unable to read block byte count.");
                }

                final int blockByteCount = (int) ByteUtil.bytesToLong(blockByteCountBuffer.toReverseEndian());
                final MutableByteArray blockHeaderBuffer = new MutableByteArray(BlockHeaderInflater.BLOCK_HEADER_BYTE_COUNT);
                final long bytesReadCount = fileInputStream.read(blockHeaderBuffer.unwrap());
                if (blockHeaderBuffer.getByteCount() != bytesReadCount) {
                    throw new Exception("Invalid blkDat " + blkDatFile + ". Unexpected end of stream while parsing block header.");
                }

                final BlockHeader blockHeader = _blockHeaderInflater.fromBytes(blockHeaderBuffer);
                final Sha256Hash blockHash = blockHeader.getHash();

                final int byteSkipCount = (blockByteCount - blockHeaderBuffer.getByteCount());
                final long byteCountSkipped = fileInputStream.skip(byteSkipCount);
                if (byteSkipCount != byteCountSkipped) {
                    throw new Exception("Invalid blkDat " + blkDatFile + ". Unexpected end of stream.");
                }

                fileOffset += blockByteCount;

                filePointers.add(new Tuple<>(blockHash, filePointer));
            }
        }

        return filePointers;
    }

    public BlkDatIndexer(final String blkDatDirectory, final Callback callback, final BlockHeaderInflater blockHeaderInflater) {
        _blockHeaderInflater = blockHeaderInflater;
        _blkDatDirectory = (blkDatDirectory + (blkDatDirectory.endsWith(File.separator) ? "" : File.separator));
        _callback = callback;
    }

    public void run() throws Exception {
        try (final DirectoryStream<Path> blkDatPaths = Files.newDirectoryStream(Paths.get(_blkDatDirectory))) {
            for (final Path blkDatPath : blkDatPaths) {
                final File blkDatFile = blkDatPath.toFile();
                if (! blkDatFile.isFile()) { continue; }

                final String blkDatFileString = blkDatFile.getName();
                if ( (! blkDatFileString.startsWith("blk")) || (! blkDatFileString.endsWith(".dat")) ) {
                    continue;
                }

                final List<Tuple<Sha256Hash, BlkDatIndex.FilePointer>> filePointers = _readBlkDat(blkDatFile);
                _callback.onNewBlkDatIndices(filePointers);
            }
        }
    }
}
