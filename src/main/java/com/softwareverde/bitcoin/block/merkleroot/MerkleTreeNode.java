package com.softwareverde.bitcoin.block.merkleroot;

import com.softwareverde.bitcoin.type.hash.Hash;
import com.softwareverde.bitcoin.type.hash.ImmutableHash;
import com.softwareverde.bitcoin.type.hash.MutableHash;
import com.softwareverde.bitcoin.type.merkleroot.ImmutableMerkleRoot;
import com.softwareverde.bitcoin.type.merkleroot.MerkleRoot;
import com.softwareverde.bitcoin.util.BitcoinUtil;
import com.softwareverde.bitcoin.util.ByteUtil;
import com.softwareverde.io.Logger;

public class MerkleTreeNode implements MerkleTree {
    protected static final ThreadLocal<byte[]> _threadLocalScratchSpace = new ThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() {
            return new byte[Hash.BYTE_COUNT * 2];
        }
    };

    protected static byte[] _calculateNodeHash(final Hash hash0, final Hash hash1) {
        final byte[] scratchSpace = _threadLocalScratchSpace.get();

        ByteUtil.setBytes(scratchSpace, hash0.toReversedEndian().getBytes());
        ByteUtil.setBytes(scratchSpace, hash1.toReversedEndian().getBytes(), Hash.BYTE_COUNT);

        return ByteUtil.reverseEndian(BitcoinUtil.sha256(BitcoinUtil.sha256(scratchSpace)));
    }

    protected Boolean _hashIsValid = false;
    protected final MutableHash _hash = new MutableHash();

    protected int _size = 0;

    protected Hashable _item0 = null;
    protected Hashable _item1 = null;

    protected MerkleTreeNode _childNode0 = null;
    protected MerkleTreeNode _childNode1 = null;

    protected void _recalculateHash() {
        final Hash hash0;
        final Hash hash1;
        {
            if (_size == 0) {
                hash0 = new ImmutableHash();
                hash1 = hash0;
            }
            else if (_item0 != null) {
                hash0 = _item0.getHash();
                hash1 = (_item1 == null ? hash0 : _item1.getHash());
            }
            else {
                hash0 = new ImmutableHash(_childNode0._getIntermediaryHash());
                hash1 = ((_childNode1 == null || _childNode1.isEmpty()) ? hash0 : new ImmutableHash(_childNode1._getIntermediaryHash()));
            }
        }

        _hash.setBytes(_calculateNodeHash(hash0, hash1));
        _hashIsValid = true;
    }

    protected MerkleTreeNode(final MerkleTreeNode childNode0, final MerkleTreeNode childNode1) {
        _childNode0 = childNode0;
        _childNode1 = childNode1;

        _size += (childNode0 == null ? 0 : childNode0.getSize());
        _size += (childNode1 == null ? 0 : childNode1.getSize());

        _hashIsValid = false;
    }

    protected MerkleTreeNode(final Hashable item0, final Hashable item1) {
        _item0 = item0;
        _item1 = item1;

        _size += (item0 == null ? 0 : 1);
        _size += (item1 == null ? 0 : 1);

        _hashIsValid = false;
    }

    protected boolean _isBalanced() {
        if (_item0 != null) {
            return (_item1 != null);
        }
        else if (_childNode0 != null) {
            final int childNode1Size = ((_childNode1 == null) ? 0 : _childNode1.getSize());
            return (_childNode0.getSize() == childNode1Size);
        }
        return true; // Is empty...
    }

    protected MerkleTreeNode _createChildNodeOfEqualDepth(final Hashable item) {
        final int depth = BitcoinUtil.log2(_size) - 1;

        final MerkleTreeNode nodeOfEqualDepthToChildNode0;
        {
            MerkleTreeNode merkleTreeNode = new MerkleTreeNode(item, null);
            for (int i = 0; i < depth; ++i) {
                merkleTreeNode = new MerkleTreeNode(merkleTreeNode, null);
            }
            nodeOfEqualDepthToChildNode0 = merkleTreeNode;
        }

        return nodeOfEqualDepthToChildNode0;
    }

    protected Hash _getIntermediaryHash() {
        if (! _hashIsValid) {
            _recalculateHash();
        }

        return _hash;
    }

    public MerkleTreeNode() {
        _hashIsValid = false;
    }

    public int getSize() {
        return _size;
    }

    public boolean isEmpty() {
        return (_size == 0);
    }

    public void clear() {
        _size = 0;
        _hashIsValid = false;
        _item0 = null;
        _item1 = null;
        _childNode0 = null;
        _childNode1 = null;
    }

    @Override
    public void addItem(final Hashable item) {
        if (_size == 0) {
            _item0 = item;
        }
        else if (_item0 != null) {
            if (_item1 == null) {
                _item1 = item;
            }
            else {
                _childNode0 = new MerkleTreeNode(_item0, _item1);
                _childNode1 = new MerkleTreeNode(item, null);

                _item0 = null;
                _item1 = null;
            }
        }
        else {
            if (_isBalanced()) {
                final MerkleTreeNode newMerkleTreeNode = new MerkleTreeNode(_childNode0, _childNode1);
                _childNode0 = newMerkleTreeNode;
                _childNode1 = _createChildNodeOfEqualDepth(item);
            }
            else {
                if (_childNode0._isBalanced()) {
                    if (_childNode1 == null) {
                        _childNode1 = _createChildNodeOfEqualDepth(item);
                    }
                    else {
                        _childNode1.addItem(item);
                    }
                }
                else {
                    _childNode0.addItem(item);
                }
            }
        }

        _size += 1;
        _hashIsValid = false;
    }

    @Override
    public MerkleRoot getMerkleRoot() {
        if ((_size == 1) && (_item0 != null)) {
            if (! _hashIsValid) {
                _hash.setBytes(_item0.getHash().getBytes());
                _hashIsValid = true;
            }
        }
        else {
            if (! _hashIsValid) {
                _recalculateHash();
            }
        }

        return new ImmutableMerkleRoot(_hash.getBytes());
    }
}