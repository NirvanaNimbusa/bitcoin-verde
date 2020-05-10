package com.softwareverde.bitcoin.transaction.script.slp.commit;

import com.softwareverde.security.hash.sha256.Sha256Hash;
import com.softwareverde.bitcoin.merkleroot.MerkleRoot;
import com.softwareverde.bitcoin.slp.SlpTokenId;

public class MutableSlpCommitScript extends SlpCommitScriptCore {

    public MutableSlpCommitScript() { }

    public MutableSlpCommitScript(final SlpCommitScript slpSendScript) {
        super(slpSendScript);
    }

    public void setTokenId(final SlpTokenId tokenId) {
        _tokenId = tokenId;
    }

    public void setBlockHash(final Sha256Hash blockHash) {
        _blockHash = blockHash;
    }

    public void setBlockHeight(final Long blockHeight) {
        _blockHeight = blockHeight;
    }

    public void setMerkleRoot(final MerkleRoot merkleRoot) {
        _merkleRoot = merkleRoot;
    }

    public void setMerkleTreeUrl(final String merkleTreeUrl) {
        _merkleTreeUrl = merkleTreeUrl;
    }

    @Override
    public ImmutableSlpCommitScript asConst() {
        return new ImmutableSlpCommitScript(this);
    }
}
