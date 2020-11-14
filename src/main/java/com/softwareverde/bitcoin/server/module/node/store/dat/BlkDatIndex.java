package com.softwareverde.bitcoin.server.module.node.store.dat;

import com.softwareverde.cryptography.hash.sha256.Sha256Hash;

public interface BlkDatIndex {
    class FilePointer {
        final String blkDatFile;
        final Long fileOffset;

        public FilePointer(final String blkDatFile, final Long fileOffset) {
            this.blkDatFile = blkDatFile;
            this.fileOffset = fileOffset;
        }
    }

    FilePointer getFilePointer(Sha256Hash blockHash, Long blockHeight);
}
