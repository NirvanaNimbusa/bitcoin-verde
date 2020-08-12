package com.softwareverde.bitcoin.transaction;

import com.softwareverde.bitcoin.address.Address;
import com.softwareverde.bitcoin.transaction.input.MutableTransactionInput;
import com.softwareverde.bitcoin.transaction.input.TransactionInput;
import com.softwareverde.bitcoin.transaction.input.TransactionInputInflater;
import com.softwareverde.bitcoin.transaction.locktime.ImmutableLockTime;
import com.softwareverde.bitcoin.transaction.output.MutableTransactionOutput;
import com.softwareverde.bitcoin.transaction.output.TransactionOutput;
import com.softwareverde.bitcoin.transaction.output.TransactionOutputInflater;
import com.softwareverde.bitcoin.util.bytearray.ByteArrayReader;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.util.ByteUtil;
import com.softwareverde.util.HexUtil;
import com.softwareverde.util.bytearray.Endian;

public class TransactionInflater {
    public static final Integer MIN_BYTE_COUNT = 100;
    public static final Integer MAX_BYTE_COUNT = (int) (2L * ByteUtil.Unit.Si.MEGABYTES);

    protected MutableTransaction _fromByteArrayReader(final ByteArrayReader byteArrayReader) {
        final Integer totalByteCount = byteArrayReader.remainingByteCount();
        // if (totalByteCount < TransactionInflater.MIN_BYTE_COUNT) { return null; } // NOTE: The min Transaction size rule was activated on HF20181115 and therefore cannot be enforced here.
        if (totalByteCount > TransactionInflater.MAX_BYTE_COUNT) { return null; }

        final MutableTransaction transaction = new MutableTransaction();
        transaction._version = byteArrayReader.readLong(4, Endian.LITTLE);

        final TransactionInputInflater transactionInputInflater = new TransactionInputInflater();
        final Long transactionInputCount = byteArrayReader.readVariableSizedInteger();
        for (int i = 0; i < transactionInputCount; ++i) {
            if (byteArrayReader.remainingByteCount() < 1) { return null; }
            final MutableTransactionInput transactionInput = transactionInputInflater.fromBytes(byteArrayReader);
            if (transactionInput == null) { return null; }
            transaction._transactionInputs.add(transactionInput);
        }

        final TransactionOutputInflater transactionOutputInflater = new TransactionOutputInflater();
        final Long transactionOutputCount = byteArrayReader.readVariableSizedInteger();
        for (int i = 0; i < transactionOutputCount; ++i) {
            if (byteArrayReader.remainingByteCount() < 1) { return null; }
            final MutableTransactionOutput transactionOutput = transactionOutputInflater.fromBytes(i, byteArrayReader);
            if (transactionOutput == null) { return null; }
            transaction._transactionOutputs.add(transactionOutput);
        }

        {
            final Long lockTimeValue = byteArrayReader.readLong(4, Endian.LITTLE);
            transaction._lockTime = new ImmutableLockTime(lockTimeValue);
        }

        if (byteArrayReader.didOverflow()) { return null; }

        return transaction;
    }

    public void debugBytes(final ByteArrayReader byteArrayReader) {
        System.out.println("Version: " + HexUtil.toHexString(byteArrayReader.readBytes(4)));

        {
            final ByteArrayReader.VariableSizedInteger inputCount = byteArrayReader.peakVariableSizedInteger();
            System.out.println("Tx Input Count: " + HexUtil.toHexString(byteArrayReader.readBytes(inputCount.bytesConsumedCount)));

            final TransactionInputInflater transactionInputInflater = new TransactionInputInflater();
            for (int i = 0; i < inputCount.value; ++i) {
                transactionInputInflater._debugBytes(byteArrayReader);
            }
        }

        {
            final ByteArrayReader.VariableSizedInteger outputCount = byteArrayReader.peakVariableSizedInteger();
            System.out.println("Tx Output Count: " + HexUtil.toHexString(byteArrayReader.readBytes(outputCount.bytesConsumedCount)));
            final TransactionOutputInflater transactionOutputInflater = new TransactionOutputInflater();
            for (int i = 0; i < outputCount.value; ++i) {
                transactionOutputInflater._debugBytes(byteArrayReader);
            }
        }

        System.out.println("LockTime: " + HexUtil.toHexString(byteArrayReader.readBytes(4)));
    }

    public Transaction fromBytes(final ByteArrayReader byteArrayReader) {
        if (byteArrayReader == null) { return null; }

        return _fromByteArrayReader(byteArrayReader);
    }

    public Transaction fromBytes(final byte[] bytes) {
        if (bytes == null) { return null; }

        final ByteArrayReader byteArrayReader = new ByteArrayReader(bytes);
        return _fromByteArrayReader(byteArrayReader);
    }

    public Transaction fromBytes(final ByteArray bytes) {
        if (bytes == null) { return null; }

        final ByteArrayReader byteArrayReader = new ByteArrayReader(bytes);
        return _fromByteArrayReader(byteArrayReader);
    }

    public Transaction createCoinbaseTransaction(final Long blockHeight, final String coinbaseMessage, final Address address, final Long satoshis) {
        final MutableTransaction coinbaseTransaction = new MutableTransaction();
        coinbaseTransaction.addTransactionInput(TransactionInput.createCoinbaseTransactionInput(blockHeight, coinbaseMessage));
        coinbaseTransaction.addTransactionOutput(TransactionOutput.createPayToAddressTransactionOutput(address, satoshis));
        return coinbaseTransaction;
    }

    public Transaction createCoinbaseTransactionWithExtraNonce(final Long blockHeight, final String coinbaseMessage, final Integer extraNonceByteCount, final Address address, final Long satoshis) {
        final MutableTransaction coinbaseTransaction = new MutableTransaction();
        coinbaseTransaction.addTransactionInput(TransactionInput.createCoinbaseTransactionInputWithExtraNonce(blockHeight, coinbaseMessage, extraNonceByteCount));
        coinbaseTransaction.addTransactionOutput(TransactionOutput.createPayToAddressTransactionOutput(address, satoshis));
        return coinbaseTransaction;
    }
}
