package com.softwareverde.bitcoin.server.message.type.node.pong;

import com.softwareverde.bitcoin.server.message.BitcoinProtocolMessage;
import com.softwareverde.bitcoin.server.message.type.MessageType;
import com.softwareverde.bitcoin.util.ByteUtil;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.network.p2p.message.type.PongMessage;
import com.softwareverde.util.bytearray.ByteArrayBuilder;
import com.softwareverde.util.bytearray.Endian;

public class BitcoinPongMessage extends BitcoinProtocolMessage implements PongMessage {

    protected Long _nonce;

    public BitcoinPongMessage() {
        super(MessageType.PONG);

        _nonce = (long) (Math.random() * Long.MAX_VALUE);
    }

    public void setNonce(final Long nonce) {
        _nonce = nonce;
    }

    @Override
    public Long getNonce() { return _nonce; }

    @Override
    protected ByteArray _getPayload() {

        final byte[] nonce = new byte[8];
        ByteUtil.setBytes(nonce, ByteUtil.longToBytes(_nonce));

        final ByteArrayBuilder byteArrayBuilder = new ByteArrayBuilder();
        byteArrayBuilder.appendBytes(nonce, Endian.LITTLE);
        return byteArrayBuilder;
    }

    @Override
    protected Integer _getPayloadByteCount() {
        return 8;
    }
}
