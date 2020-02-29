package com.softwareverde.bitcoin.transaction.validator;

import com.softwareverde.bitcoin.address.Address;
import com.softwareverde.bitcoin.address.AddressInflater;
import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockId;
import com.softwareverde.bitcoin.block.BlockInflater;
import com.softwareverde.bitcoin.block.MutableBlock;
import com.softwareverde.bitcoin.chain.segment.BlockchainSegmentId;
import com.softwareverde.bitcoin.chain.time.ImmutableMedianBlockTime;
import com.softwareverde.bitcoin.chain.time.MutableMedianBlockTime;
import com.softwareverde.security.hash.sha256.MutableSha256Hash;
import com.softwareverde.security.secp256k1.key.PrivateKey;
import com.softwareverde.bitcoin.server.database.DatabaseConnection;
import com.softwareverde.bitcoin.server.database.query.Query;
import com.softwareverde.bitcoin.server.module.node.database.DatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.block.fullnode.FullNodeBlockDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.block.header.BlockHeaderDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.fullnode.FullNodeDatabaseManager;
import com.softwareverde.bitcoin.server.module.node.database.transaction.fullnode.FullNodeTransactionDatabaseManager;
import com.softwareverde.bitcoin.test.BlockData;
import com.softwareverde.bitcoin.test.IntegrationTest;
import com.softwareverde.bitcoin.test.TransactionTestUtil;
import com.softwareverde.bitcoin.transaction.MutableTransaction;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.TransactionId;
import com.softwareverde.bitcoin.transaction.TransactionInflater;
import com.softwareverde.bitcoin.transaction.input.MutableTransactionInput;
import com.softwareverde.bitcoin.transaction.input.TransactionInput;
import com.softwareverde.bitcoin.transaction.locktime.ImmutableLockTime;
import com.softwareverde.bitcoin.transaction.locktime.LockTime;
import com.softwareverde.bitcoin.transaction.locktime.SequenceNumber;
import com.softwareverde.bitcoin.transaction.output.MutableTransactionOutput;
import com.softwareverde.bitcoin.transaction.output.TransactionOutput;
import com.softwareverde.bitcoin.transaction.script.ScriptBuilder;
import com.softwareverde.bitcoin.transaction.script.unlocking.UnlockingScript;
import com.softwareverde.bitcoin.transaction.signer.*;
import com.softwareverde.bitcoin.wallet.PaymentAmount;
import com.softwareverde.bitcoin.wallet.Wallet;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.database.DatabaseException;
import com.softwareverde.network.time.ImmutableNetworkTime;
import com.softwareverde.util.HexUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TransactionValidatorTests extends IntegrationTest {
    static class StoredBlock {
        public final BlockId blockId;
        public final Block block;

        public StoredBlock(final BlockId blockId, final Block block) {
            this.blockId = blockId;
            this.block = block;
        }
    }

    public static class TestBlockDatabaseManager extends FullNodeBlockDatabaseManager {
        public TestBlockDatabaseManager(final FullNodeDatabaseManager databaseManager) {
            super(databaseManager);
        }

        public void associateTransactionToBlock(final TransactionId transactionId, final BlockId blockId) throws DatabaseException {
            _associateTransactionToBlock(transactionId, blockId);
        }
    }

    protected StoredBlock _storeBlock(final String blockBytes) throws DatabaseException {
        try (final FullNodeDatabaseManager databaseManager = _fullNodeDatabaseManagerFactory.newDatabaseManager()) {
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();
            final FullNodeBlockDatabaseManager blockDatabaseManager = databaseManager.getBlockDatabaseManager();
            final BlockInflater blockInflater = new BlockInflater();
            final Block block = blockInflater.fromBytes(HexUtil.hexStringToByteArray(blockBytes));
            blockDatabaseManager.insertBlock(block);
            return new StoredBlock(blockHeaderDatabaseManager.getBlockHeaderId(block.getHash()), block);
        }
    }

    public static MutableTransactionOutput _createTransactionOutput(final Address payToAddress, final Long amount) {
        final MutableTransactionOutput transactionOutput = new MutableTransactionOutput();
        transactionOutput.setAmount(amount);
        transactionOutput.setIndex(0);
        transactionOutput.setLockingScript((ScriptBuilder.payToAddress(payToAddress)));
        return transactionOutput;
    }

    public static TransactionInput _createCoinbaseTransactionInput() {
        final MutableTransactionInput mutableTransactionInput = new MutableTransactionInput();
        mutableTransactionInput.setPreviousOutputTransactionHash(new MutableSha256Hash());
        mutableTransactionInput.setPreviousOutputIndex(-1);
        mutableTransactionInput.setSequenceNumber(SequenceNumber.MAX_SEQUENCE_NUMBER);
        mutableTransactionInput.setUnlockingScript((new ScriptBuilder()).pushString("Mined via Bitcoin-Verde.").buildUnlockingScript());
        return mutableTransactionInput;
    }

    public static MutableTransactionInput _createTransactionInputThatSpendsTransaction(final Transaction transactionToSpend) {
        final MutableTransactionInput mutableTransactionInput = new MutableTransactionInput();
        mutableTransactionInput.setPreviousOutputTransactionHash(transactionToSpend.getHash());
        mutableTransactionInput.setPreviousOutputIndex(0);
        mutableTransactionInput.setSequenceNumber(SequenceNumber.MAX_SEQUENCE_NUMBER);
        mutableTransactionInput.setUnlockingScript(UnlockingScript.EMPTY_SCRIPT);
        return mutableTransactionInput;
    }

    public static MutableTransaction _createTransactionContaining(final TransactionInput transactionInput, final TransactionOutput transactionOutput) {
        final MutableTransaction mutableTransaction = new MutableTransaction();
        mutableTransaction.setVersion(1L);
        mutableTransaction.setLockTime(new ImmutableLockTime(LockTime.MIN_TIMESTAMP));

        mutableTransaction.addTransactionInput(transactionInput);
        mutableTransaction.addTransactionOutput(transactionOutput);

        return mutableTransaction;
    }

    public static Long calculateBlockHeight(final DatabaseManager databaseManager) throws DatabaseException {
        final DatabaseConnection databaseConnection = databaseManager.getDatabaseConnection();
        return databaseConnection.query(new Query("SELECT COUNT(*) AS block_height FROM blocks")).get(0).getLong("block_height");
    }

    @Before
    public void setup() {
        _resetDatabase();
    }

    @Test
    public void should_validate_valid_transaction() throws Exception {
        // Setup
        try (final FullNodeDatabaseManager databaseManager = _fullNodeDatabaseManagerFactory.newDatabaseManager()) {
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();
            final TestBlockDatabaseManager blockDatabaseManager = new TestBlockDatabaseManager(databaseManager);
            final FullNodeTransactionDatabaseManager transactionDatabaseManager = databaseManager.getTransactionDatabaseManager();
            final TransactionValidator transactionValidator = new TransactionValidatorCore(databaseManager, new ImmutableNetworkTime(Long.MAX_VALUE), new ImmutableMedianBlockTime(Long.MAX_VALUE));

            final TransactionInflater transactionInflater = new TransactionInflater();

            final BlockchainSegmentId blockchainSegmentId;

            synchronized (BlockHeaderDatabaseManager.MUTEX) { // Store the transaction output being spent by the transaction...
                _storeBlock(BlockData.MainChain.GENESIS_BLOCK);
                final StoredBlock storedBlock = _storeBlock(BlockData.MainChain.BLOCK_1);
                blockchainSegmentId = blockHeaderDatabaseManager.getBlockchainSegmentId(storedBlock.blockId);
                final Transaction previousTransaction = transactionInflater.fromBytes(HexUtil.hexStringToByteArray("0100000001E7FCF39EE6B86F1595C55B16B60BF4F297988CB9519F5D42597E7FB721E591C6010000008B483045022100AC572B43E78089851202CFD9386750B08AFC175318C537F04EB364BF5A0070D402203F0E829D4BAEA982FEAF987CB9F14C85097D2FBE89FBA3F283F6925B3214A97E0141048922FA4DC891F9BB39F315635C03E60E019FF9EC1559C8B581324B4C3B7589A57550F9B0B80BC72D0F959FDDF6CA65F07223C37A8499076BD7027AE5C325FAC5FFFFFFFF0140420F00000000001976A914C4EB47ECFDCF609A1848EE79ACC2FA49D3CAAD7088AC00000000"));
                TransactionTestUtil.createRequiredTransactionInputs(databaseManager, blockchainSegmentId, previousTransaction);
                final TransactionId transactionId = transactionDatabaseManager.storeTransaction(previousTransaction);
                blockDatabaseManager.associateTransactionToBlock(transactionId, storedBlock.blockId);
            }

            final byte[] transactionBytes = HexUtil.hexStringToByteArray("01000000010B6072B386D4A773235237F64C1126AC3B240C84B917A3909BA1C43DED5F51F4000000008C493046022100BB1AD26DF930A51CCE110CF44F7A48C3C561FD977500B1AE5D6B6FD13D0B3F4A022100C5B42951ACEDFF14ABBA2736FD574BDB465F3E6F8DA12E2C5303954ACA7F78F3014104A7135BFE824C97ECC01EC7D7E336185C81E2AA2C41AB175407C09484CE9694B44953FCB751206564A9C24DD094D42FDBFDD5AAD3E063CE6AF4CFAAEA4EA14FBBFFFFFFFF0140420F00000000001976A91439AA3D569E06A1D7926DC4BE1193C99BF2EB9EE088AC00000000");
            final Transaction transaction = transactionInflater.fromBytes(transactionBytes);
            transactionDatabaseManager.storeTransaction(transaction);

            // Action
            final Boolean inputsAreUnlocked = transactionValidator.validateTransaction(blockchainSegmentId, TransactionValidatorTests.calculateBlockHeight(databaseManager), transaction, true);

            // Assert
            Assert.assertTrue(inputsAreUnlocked);
        }
    }

    @Test
    public void should_create_signed_transaction_and_unlock_it() throws Exception {
        // Setup
        try (final FullNodeDatabaseManager databaseManager = _fullNodeDatabaseManagerFactory.newDatabaseManager()) {
            final FullNodeTransactionDatabaseManager transactionDatabaseManager = databaseManager.getTransactionDatabaseManager();
            final TransactionValidator transactionValidator = new TransactionValidatorCore(databaseManager, new ImmutableNetworkTime(Long.MAX_VALUE), new ImmutableMedianBlockTime(Long.MAX_VALUE));

            final AddressInflater addressInflater = new AddressInflater();
            final TransactionSigner transactionSigner = new TransactionSigner();
            final PrivateKey privateKey = PrivateKey.createNewKey();

            // Create a transaction that will be spent in our signed transaction.
            //  This transaction will create an output that can be spent by our private key.
            final Transaction transactionToSpend = _createTransactionContaining(
                _createCoinbaseTransactionInput(),
                _createTransactionOutput(addressInflater.uncompressedFromPrivateKey(privateKey), 50L * Transaction.SATOSHIS_PER_BITCOIN)
            );

            // Store the transaction in the database so that our validator can access it.
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();
            final TestBlockDatabaseManager blockDatabaseManager = new TestBlockDatabaseManager(databaseManager);
            final StoredBlock storedBlock;
            synchronized (BlockHeaderDatabaseManager.MUTEX) {
                _storeBlock(BlockData.MainChain.GENESIS_BLOCK);
                storedBlock = _storeBlock(BlockData.MainChain.BLOCK_1);
            }
            final BlockchainSegmentId blockchainSegmentId = blockHeaderDatabaseManager.getBlockchainSegmentId(storedBlock.blockId);
            final TransactionId transactionId = transactionDatabaseManager.storeTransaction(transactionToSpend);
            blockDatabaseManager.associateTransactionToBlock(transactionId, storedBlock.blockId);

            // Create an unsigned transaction that spends our previous transaction, and send our payment to an irrelevant address.
            final Transaction unsignedTransaction = _createTransactionContaining(
                _createTransactionInputThatSpendsTransaction(transactionToSpend),
                _createTransactionOutput(addressInflater.uncompressedFromBase58Check("1HrXm9WZF7LBm3HCwCBgVS3siDbk5DYCuW"), 50L * Transaction.SATOSHIS_PER_BITCOIN)
            );

            final TransactionOutputRepository transactionOutputRepository = new DatabaseTransactionOutputRepository(databaseManager);

            // Sign the unsigned transaction.
            final SignatureContextGenerator signatureContextGenerator = new SignatureContextGenerator(transactionOutputRepository);
            final SignatureContext signatureContext = signatureContextGenerator.createContextForEntireTransaction(unsignedTransaction, false);
            final Transaction signedTransaction = transactionSigner.signTransaction(signatureContext, privateKey);
            transactionDatabaseManager.storeTransaction(signedTransaction);

            // Action
            final Boolean inputsAreUnlocked = transactionValidator.validateTransaction(blockchainSegmentId, TransactionValidatorTests.calculateBlockHeight(databaseManager), signedTransaction, true);

            // Assert
            Assert.assertTrue(inputsAreUnlocked);
        }
    }

    @Test
    public void should_detect_an_address_attempting_to_spend_an_output_it_cannot_unlock() throws Exception {
        // Setup
        try (final FullNodeDatabaseManager databaseManager = _fullNodeDatabaseManagerFactory.newDatabaseManager()) {
            final AddressInflater addressInflater = new AddressInflater();
            final TransactionSigner transactionSigner = new TransactionSigner();
            final FullNodeTransactionDatabaseManager transactionDatabaseManager = databaseManager.getTransactionDatabaseManager();
            final TransactionValidator transactionValidator = new TransactionValidatorCore(databaseManager, new ImmutableNetworkTime(Long.MAX_VALUE), new ImmutableMedianBlockTime(Long.MAX_VALUE));
            final PrivateKey privateKey = PrivateKey.createNewKey();

            // Create a transaction that will be spent in our signed transaction.
            //  This transaction output is being sent to an address we don't have access to.
            final Transaction transactionToSpend = _createTransactionContaining(
                _createCoinbaseTransactionInput(),
                _createTransactionOutput(addressInflater.uncompressedFromPrivateKey(PrivateKey.createNewKey()), 50L * Transaction.SATOSHIS_PER_BITCOIN)
            );

            // Store the transaction in the database so that our validator can access it.
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();
            final TestBlockDatabaseManager blockDatabaseManager = new TestBlockDatabaseManager(databaseManager);
            final StoredBlock storedBlock;
            synchronized (BlockHeaderDatabaseManager.MUTEX) {
                _storeBlock(BlockData.MainChain.GENESIS_BLOCK);
                storedBlock = _storeBlock(BlockData.MainChain.BLOCK_1);
            }
            final BlockchainSegmentId blockchainSegmentId = blockHeaderDatabaseManager.getBlockchainSegmentId(storedBlock.blockId);
            final TransactionId transactionId = transactionDatabaseManager.storeTransaction(transactionToSpend);
            blockDatabaseManager.associateTransactionToBlock(transactionId, storedBlock.blockId);

            // Create an unsigned transaction that spends our previous transaction, and send our payment to an irrelevant address.
            final Transaction unsignedTransaction = _createTransactionContaining(
                _createTransactionInputThatSpendsTransaction(transactionToSpend),
                _createTransactionOutput(addressInflater.uncompressedFromBase58Check("1HrXm9WZF7LBm3HCwCBgVS3siDbk5DYCuW"), 50L * Transaction.SATOSHIS_PER_BITCOIN)
            );

            final TransactionOutputRepository transactionOutputRepository = new DatabaseTransactionOutputRepository(databaseManager);

            // Sign the unsigned transaction with our key that does not match the address given to transactionToSpend.
            final SignatureContextGenerator signatureContextGenerator = new SignatureContextGenerator(transactionOutputRepository);
            final SignatureContext signatureContext = signatureContextGenerator.createContextForEntireTransaction(unsignedTransaction, false);
            final Transaction signedTransaction = transactionSigner.signTransaction(signatureContext, privateKey);

            // Action
            final Boolean inputsAreUnlocked = transactionValidator.validateTransaction(blockchainSegmentId, TransactionValidatorTests.calculateBlockHeight(databaseManager), signedTransaction, true);

            // Assert
            Assert.assertFalse(inputsAreUnlocked);
        }
    }

    @Test
    public void should_detect_an_address_attempting_to_spend_an_output_with_the_incorrect_signature() throws Exception {
        try (final FullNodeDatabaseManager databaseManager = _fullNodeDatabaseManagerFactory.newDatabaseManager()) {
            // Setup
            final AddressInflater addressInflater = new AddressInflater();
            final TransactionSigner transactionSigner = new TransactionSigner();
            final FullNodeTransactionDatabaseManager transactionDatabaseManager = databaseManager.getTransactionDatabaseManager();
            final TransactionValidator transactionValidator = new TransactionValidatorCore(databaseManager, new ImmutableNetworkTime(Long.MAX_VALUE), new ImmutableMedianBlockTime(Long.MAX_VALUE));
            final PrivateKey privateKey = PrivateKey.createNewKey();

            // Create a transaction that will be spent in our signed transaction.
            //  This transaction output is being sent to an address we should have access to.
            final Transaction transactionToSpend = _createTransactionContaining(
                _createCoinbaseTransactionInput(),
                _createTransactionOutput(addressInflater.uncompressedFromPrivateKey(privateKey), 50L * Transaction.SATOSHIS_PER_BITCOIN)
            );

            // Store the transaction in the database so that our validator can access it.
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();
            final TestBlockDatabaseManager blockDatabaseManager = new TestBlockDatabaseManager(databaseManager);
            final StoredBlock storedBlock;
            synchronized (BlockHeaderDatabaseManager.MUTEX) {
                _storeBlock(BlockData.MainChain.GENESIS_BLOCK);
                storedBlock = _storeBlock(BlockData.MainChain.BLOCK_1);
            }
            final BlockchainSegmentId blockchainSegmentId = blockHeaderDatabaseManager.getBlockchainSegmentId(storedBlock.blockId);
            final TransactionId transactionId = transactionDatabaseManager.storeTransaction(transactionToSpend);
            blockDatabaseManager.associateTransactionToBlock(transactionId, storedBlock.blockId);

            // Create an unsigned transaction that spends our previous transaction, and send our payment to an irrelevant address.
            final Transaction unsignedTransaction = _createTransactionContaining(
                _createTransactionInputThatSpendsTransaction(transactionToSpend),
                _createTransactionOutput(addressInflater.uncompressedFromBase58Check("1HrXm9WZF7LBm3HCwCBgVS3siDbk5DYCuW"), 50L * Transaction.SATOSHIS_PER_BITCOIN)
            );

            final TransactionOutputRepository transactionOutputRepository = new DatabaseTransactionOutputRepository(databaseManager);

            // Sign the unsigned transaction with our key that does not match the signature given to transactionToSpend.
            final SignatureContextGenerator signatureContextGenerator = new SignatureContextGenerator(transactionOutputRepository);
            final SignatureContext signatureContext = signatureContextGenerator.createContextForEntireTransaction(unsignedTransaction, false);
            final Transaction signedTransaction = transactionSigner.signTransaction(signatureContext, PrivateKey.createNewKey());

            // Action
            final Boolean inputsAreUnlocked = transactionValidator.validateTransaction(blockchainSegmentId, TransactionValidatorTests.calculateBlockHeight(databaseManager), signedTransaction, true);

            // Assert
            Assert.assertFalse(inputsAreUnlocked);
        }
    }

    @Test
    public void should_not_validate_transaction_whose_inputs_spend_the_same_output() throws Exception {
        try (final FullNodeDatabaseManager databaseManager = _fullNodeDatabaseManagerFactory.newDatabaseManager()) {
            // Setup
            final AddressInflater addressInflater = new AddressInflater();
            final TransactionSigner transactionSigner = new TransactionSigner();
            final FullNodeTransactionDatabaseManager transactionDatabaseManager = databaseManager.getTransactionDatabaseManager();
            final TransactionValidator transactionValidator = new TransactionValidatorCore(databaseManager, new ImmutableNetworkTime(Long.MAX_VALUE), new ImmutableMedianBlockTime(Long.MAX_VALUE));
            final PrivateKey privateKey = PrivateKey.createNewKey();

            // Create a transaction that will be spent in our signed transaction.
            //  This transaction will create an output that can be spent by our private key.
            final Transaction transactionToSpend = _createTransactionContaining(
                _createCoinbaseTransactionInput(),
                _createTransactionOutput(addressInflater.uncompressedFromPrivateKey(privateKey), 50L * Transaction.SATOSHIS_PER_BITCOIN)
            );

            // Store the transaction in the database so that our validator can access it.
            final BlockHeaderDatabaseManager blockHeaderDatabaseManager = databaseManager.getBlockHeaderDatabaseManager();
            final TestBlockDatabaseManager blockDatabaseManager = new TestBlockDatabaseManager(databaseManager);
            final StoredBlock storedBlock;
            synchronized (BlockHeaderDatabaseManager.MUTEX) {
                _storeBlock(BlockData.MainChain.GENESIS_BLOCK);
                storedBlock = _storeBlock(BlockData.MainChain.BLOCK_1);
            }
            final BlockchainSegmentId blockchainSegmentId = blockHeaderDatabaseManager.getBlockchainSegmentId(storedBlock.blockId);
            final TransactionId transactionId = transactionDatabaseManager.storeTransaction(transactionToSpend);
            blockDatabaseManager.associateTransactionToBlock(transactionId, storedBlock.blockId);

            // Create an unsigned transaction that spends our previous transaction, and send our payment to an irrelevant address.
            final MutableTransaction unsignedTransaction = _createTransactionContaining(
                _createTransactionInputThatSpendsTransaction(transactionToSpend),
                _createTransactionOutput(addressInflater.uncompressedFromBase58Check("1HrXm9WZF7LBm3HCwCBgVS3siDbk5DYCuW"), 50L * Transaction.SATOSHIS_PER_BITCOIN)
            );

            // Mutate the transaction so that it attempts to spend the same output twice...
            unsignedTransaction.addTransactionInput(unsignedTransaction.getTransactionInputs().get(0));

            final TransactionOutputRepository transactionOutputRepository = new DatabaseTransactionOutputRepository(databaseManager);

            // Sign the unsigned transaction.
            final SignatureContextGenerator signatureContextGenerator = new SignatureContextGenerator(transactionOutputRepository);
            final SignatureContext signatureContext = signatureContextGenerator.createContextForEntireTransaction(unsignedTransaction, false);
            final Transaction signedTransaction = transactionSigner.signTransaction(signatureContext, privateKey);

            // Action
            final Boolean transactionIsValid;
            {
                Boolean isValid;
                try {
                    transactionDatabaseManager.storeTransaction(signedTransaction); // Should fail to insert transaction due to constraint transaction_inputs_tx_id_prev_tx_id_uq...
                    isValid = transactionValidator.validateTransaction(blockchainSegmentId, TransactionValidatorTests.calculateBlockHeight(databaseManager), signedTransaction, true);
                }
                catch (final DatabaseException exception) {
                    isValid = false;
                }
                transactionIsValid = isValid;
            }

            // Assert
            Assert.assertFalse(transactionIsValid);
        }
    }

    @Test
    public void should_not_validate_transaction_that_spends_the_same_input_twice() throws Exception {
        try (final FullNodeDatabaseManager databaseManager = _fullNodeDatabaseManagerFactory.newDatabaseManager()) {
            // Setup
            final FullNodeBlockDatabaseManager blockDatabaseManager = databaseManager.getBlockDatabaseManager();
            final BlockInflater blockInflater = new BlockInflater();
            final AddressInflater addressInflater = new AddressInflater();
            final TransactionSigner transactionSigner = new TransactionSigner();
            final TransactionValidator transactionValidator = new TransactionValidatorCore(databaseManager, new ImmutableNetworkTime(Long.MAX_VALUE), new ImmutableMedianBlockTime(Long.MAX_VALUE));
            final FullNodeTransactionDatabaseManager transactionDatabaseManager = databaseManager.getTransactionDatabaseManager();

            Block lastBlock = null;
            BlockId lastBlockId = null;
            for (final String blockData : new String[] { BlockData.MainChain.GENESIS_BLOCK, BlockData.MainChain.BLOCK_1, BlockData.MainChain.BLOCK_2 }) {
                final Block block = blockInflater.fromBytes(HexUtil.hexStringToByteArray(blockData));
                synchronized (BlockHeaderDatabaseManager.MUTEX) {
                    lastBlockId = blockDatabaseManager.storeBlock(block);
                }
                lastBlock = block;
            }
            Assert.assertNotNull(lastBlock);
            Assert.assertNotNull(lastBlockId);

            final PrivateKey privateKey = PrivateKey.createNewKey();

            final Transaction transactionToSpend;
            final MutableBlock mutableBlock = new MutableBlock();
            {
                mutableBlock.setDifficulty(lastBlock.getDifficulty());
                mutableBlock.setNonce(lastBlock.getNonce());
                mutableBlock.setTimestamp(lastBlock.getTimestamp());
                mutableBlock.setPreviousBlockHash(lastBlock.getHash());
                mutableBlock.setVersion(lastBlock.getVersion());

                // Create a transaction that will be spent in our signed transaction.
                //  This transaction will create an output that can be spent by our private key.
                transactionToSpend = _createTransactionContaining(
                    _createCoinbaseTransactionInput(),
                    _createTransactionOutput(addressInflater.uncompressedFromPrivateKey(privateKey), 1L * Transaction.SATOSHIS_PER_BITCOIN)
                );

                mutableBlock.addTransaction(transactionToSpend);

                synchronized (BlockHeaderDatabaseManager.MUTEX) {
                    blockDatabaseManager.storeBlock(mutableBlock);
                }
            }

            final Transaction signedTransaction;
            {
                // Create an unsigned transaction that spends our previous transaction, and send our payment to an irrelevant address.
                // The amount created by the input is greater than the input amount, and therefore, this Tx should not validate.
                final MutableTransaction unsignedTransaction = _createTransactionContaining(
                    _createTransactionInputThatSpendsTransaction(transactionToSpend),
                    _createTransactionOutput(addressInflater.uncompressedFromBase58Check("1HrXm9WZF7LBm3HCwCBgVS3siDbk5DYCuW"), 50L * Transaction.SATOSHIS_PER_BITCOIN)
                );

                final TransactionOutputRepository transactionOutputRepository = new DatabaseTransactionOutputRepository(databaseManager);

                // Sign the unsigned transaction.
                final SignatureContextGenerator signatureContextGenerator = new SignatureContextGenerator(transactionOutputRepository);
                final SignatureContext signatureContext = signatureContextGenerator.createContextForEntireTransaction(unsignedTransaction, false);
                signedTransaction = transactionSigner.signTransaction(signatureContext, privateKey);

                transactionDatabaseManager.storeTransaction(signedTransaction);
            }

            // Action
            final Boolean isValid = transactionValidator.validateTransaction(BlockchainSegmentId.wrap(1L), TransactionValidatorTests.calculateBlockHeight(databaseManager), signedTransaction, true);

            // Assert
            Assert.assertFalse(isValid);
        }
    }

    @Test
    public void should_not_accept_transaction_that_double_spends_output_into_mempool() throws Exception {
        try (final FullNodeDatabaseManager databaseManager = _fullNodeDatabaseManagerFactory.newDatabaseManager()) {
            // Setup
            final FullNodeBlockDatabaseManager blockDatabaseManager = databaseManager.getBlockDatabaseManager();
            final BlockInflater blockInflater = new BlockInflater();
            final AddressInflater addressInflater = new AddressInflater();
            final TransactionValidator transactionValidator = new TransactionValidatorCore(databaseManager, new ImmutableNetworkTime(Long.MAX_VALUE), new ImmutableMedianBlockTime(Long.MAX_VALUE));
            final FullNodeTransactionDatabaseManager transactionDatabaseManager = databaseManager.getTransactionDatabaseManager();

            Block lastBlock = null;
            BlockId lastBlockId = null;
            for (final String blockData : new String[] { BlockData.MainChain.GENESIS_BLOCK, BlockData.ForkChain2.BLOCK_1 }) {
                final Block block = blockInflater.fromBytes(HexUtil.hexStringToByteArray(blockData));
                synchronized (BlockHeaderDatabaseManager.MUTEX) {
                    lastBlockId = blockDatabaseManager.storeBlock(block);
                }
                lastBlock = block;

            }
            Assert.assertNotNull(lastBlock);
            Assert.assertNotNull(lastBlockId);

            final PrivateKey privateKey = PrivateKey.fromHexString("697D9CCCD7A09A31ED41C1D1BFF35E2481098FB03B4E73FAB7D4C15CF01FADCC");

            final Wallet wallet = new Wallet(new MutableMedianBlockTime());
            wallet.addPrivateKey(privateKey);
            wallet.setSatoshisPerByteFee(0D);
            wallet.addTransaction(lastBlock.getCoinbaseTransaction());

            final Transaction signedTransaction;
            {
                final MutableList<PaymentAmount> paymentAmounts = new MutableList<PaymentAmount>();
                paymentAmounts.add(new PaymentAmount(addressInflater.uncompressedFromBase58Check("1HPPterRZy2Thr8kEtd4SAennyaFFEAngV"), 50 * Transaction.SATOSHIS_PER_BITCOIN));
                signedTransaction = wallet.createTransaction(paymentAmounts, null);
            }

            final Transaction doubleSpendingSignedTransaction;
            {
                final MutableList<PaymentAmount> paymentAmounts = new MutableList<PaymentAmount>();
                paymentAmounts.add(new PaymentAmount(addressInflater.uncompressedFromBase58Check("149uLAy8vkn1Gm68t5NoLQtUqBtngjySLF"), 50 * Transaction.SATOSHIS_PER_BITCOIN));
                doubleSpendingSignedTransaction = wallet.createTransaction(paymentAmounts, null);
            }

            final TransactionId signedTransactionId = transactionDatabaseManager.storeTransaction(signedTransaction);

            final Boolean firstTransactionIsValid = transactionValidator.validateTransaction(BlockchainSegmentId.wrap(1L), Long.MAX_VALUE, signedTransaction, true);
            Assert.assertTrue(firstTransactionIsValid);

            transactionDatabaseManager.addToUnconfirmedTransactions(signedTransactionId);

            final TransactionId doubleSpendingTransactionId = transactionDatabaseManager.storeTransaction(doubleSpendingSignedTransaction);

            // Action
            final Boolean doubleSpendIsValid = transactionValidator.validateTransaction(BlockchainSegmentId.wrap(1L), Long.MAX_VALUE, doubleSpendingSignedTransaction, true);

            // Assert
            Assert.assertFalse(doubleSpendIsValid);
        }
    }
}
