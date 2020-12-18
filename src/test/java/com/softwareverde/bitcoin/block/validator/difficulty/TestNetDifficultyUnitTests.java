package com.softwareverde.bitcoin.block.validator.difficulty;

import com.softwareverde.bitcoin.bip.CoreUpgradeSchedule;
import com.softwareverde.bitcoin.bip.TestNetUpgradeSchedule;
import com.softwareverde.bitcoin.bip.UpgradeSchedule;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.BlockHeaderDeflater;
import com.softwareverde.bitcoin.block.header.BlockHeaderInflater;
import com.softwareverde.bitcoin.block.header.MutableBlockHeader;
import com.softwareverde.bitcoin.block.header.difficulty.Difficulty;
import com.softwareverde.bitcoin.block.header.difficulty.work.ChainWork;
import com.softwareverde.bitcoin.block.header.difficulty.work.MutableChainWork;
import com.softwareverde.bitcoin.chain.time.MedianBlockTime;
import com.softwareverde.bitcoin.merkleroot.ImmutableMerkleRoot;
import com.softwareverde.bitcoin.merkleroot.MerkleRoot;
import com.softwareverde.bitcoin.test.UnitTest;
import com.softwareverde.bitcoin.test.fake.FakeDifficultyCalculatorContext;
import com.softwareverde.bitcoin.util.IoUtil;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.json.Json;
import com.softwareverde.util.HexUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class TestNetDifficultyUnitTests extends UnitTest {

    protected void _setChainWork(final Long blockHeight, final String chainWorkString, final FakeDifficultyCalculatorContext context) {
        final ChainWork chainWork = ChainWork.fromHexString(chainWorkString);
        context.getChainWorks().put(blockHeight, chainWork);
    }

    protected BlockHeader _loadBlock(final Long blockHeight, final String blockData, final FakeDifficultyCalculatorContext context) {
        return _loadBlock(blockHeight, blockData, MedianBlockTime.GENESIS_BLOCK_TIMESTAMP, context, false);
    }

    protected BlockHeader _loadBlock(final Long blockHeight, final String blockData, final Long medianBlockTime, final FakeDifficultyCalculatorContext context) {
        return _loadBlock(blockHeight, blockData, medianBlockTime, context, false);
    }

    protected BlockHeader _loadBlock(final Long blockHeight, final String blockData, final FakeDifficultyCalculatorContext context, final Boolean shouldCalculateChainWork) {
        return _loadBlock(blockHeight, blockData, MedianBlockTime.GENESIS_BLOCK_TIMESTAMP, context, shouldCalculateChainWork);
    }

    protected BlockHeader _loadBlock(final Long blockHeight, final String blockData, final Long medianBlockTime, final FakeDifficultyCalculatorContext context, final Boolean shouldCalculateChainWork) {
        final BlockHeaderInflater blockHeaderInflater = new BlockHeaderInflater();
        final HashMap<Long, BlockHeader> blockHeaders = context.getBlockHeaders();
        final HashMap<Long, ChainWork> chainWorks = context.getChainWorks();
        final HashMap<Long, MedianBlockTime> medianBlockTimes = context.getMedianBlockTimes();

        final BlockHeader blockHeader = blockHeaderInflater.fromBytes(ByteArray.fromHexString(blockData));
        blockHeaders.put(blockHeight, blockHeader);

        if (shouldCalculateChainWork) {
            final MutableChainWork chainWork = new MutableChainWork(context.getChainWork(blockHeight - 1L));
            chainWork.add(blockHeader.getDifficulty().calculateWork());
            chainWorks.put(blockHeight, chainWork);
        }

        medianBlockTimes.put(blockHeight, MedianBlockTime.fromSeconds(medianBlockTime));

        return blockHeader;
    }

    @Test
    public void should_calculate_difficulty_for_block_000000001AF3B22A7598B10574DEB6B3E2D596F36D62B0A49CB89A1F99AB81EB() {
        // Setup
        final UpgradeSchedule upgradeSchedule = new TestNetUpgradeSchedule();
        final FakeDifficultyCalculatorContext difficultyCalculatorContext = new FakeDifficultyCalculatorContext(upgradeSchedule);

        _loadBlock(
            2016L,
            "01000000299B05FA50212725C6082936BB1CEDE9164AAA36103325504C744B8600000000F480BD5081532A426C15BF3AA534A0817B21217CE21DACA4EA87AC509637D670EABEBF4FFFFF001D04E6838E0101000000010000000000000000000000000000000000000000000000000000000000000000FFFFFFFF1F04EABEBF4F010B172F503253482F49636549726F6E2D51432D6D696E65722FFFFFFFFF0100F2052A01000000232103894E257ABCCA0889212F3648867CBAE0C06EA5413A8BC936ED61740D088642EEAC00000000",
            difficultyCalculatorContext
        );

        { // Simulate the real-scenario of 6 blocks every second in between blocks 4028 and 2016.
            final BlockHeaderInflater blockHeaderInflater = new BlockHeaderInflater();
            final HashMap<Long, BlockHeader> blockHeaders = difficultyCalculatorContext.getBlockHeaders();
            final HashMap<Long, MedianBlockTime> medianBlockTimes = difficultyCalculatorContext.getMedianBlockTimes();

            long timestamp = -1L;
            int timestampHackCount = 3;
            for (long blockHeight = 4028L; blockHeight > 2016L; --blockHeight) {
                final MutableBlockHeader blockHeader = blockHeaderInflater.fromBytes(ByteArray.fromHexString("010000000FFE4DA03846C3108795EE9896CFD9637E94901F36E72C161658153500000000F6CE8D7B5DE8EB63F1318EDB97C6BE1E4BF8E13F2AA555FC71169748FC82DDC139C0BF4FFFFF001D03465A9A0101000000010000000000000000000000000000000000000000000000000000000000000000FFFFFFFF1F0439C0BF4F0105172F503253482F49636549726F6E2D51432D6D696E65722FFFFFFFFF0100F2052A0100000023210337B7371A46331F499D68CDC264556C4CE0D45B9B505BC974B0C4BAA9AF109326AC00000000"));
                if (timestamp < 0L) {
                    timestamp = blockHeader.getTimestamp();
                }

                blockHeader.setTimestamp(timestamp);
                blockHeaders.put(blockHeight, blockHeader);
                medianBlockTimes.put(blockHeight, MedianBlockTime.fromSeconds(MedianBlockTime.GENESIS_BLOCK_TIMESTAMP));

                timestampHackCount += 1;

                if (timestampHackCount % 6 == 0) {
                    timestamp = (timestamp - 1L);
                }
            }
        }

        _loadBlock(
            4029L,
            "01000000C6F46128AD4E2DDDFA409A73D5B06C12E465816AB1B76700154912F5000000007F9D342E48137F30259C8B4B922EC3459E304D7A07C1C443E677059F2C7221A939C0BF4FFFFF001D100B78190101000000010000000000000000000000000000000000000000000000000000000000000000FFFFFFFF1F0439C0BF4F0103172F503253482F49636549726F6E2D51432D6D696E65722FFFFFFFFF0100F2052A01000000232102539E7861074EE75EB5C8EA4A7DD616674F12E7DDA6C431BFF83E0B3410F528CEAC00000000",
            difficultyCalculatorContext
        );
        _loadBlock(
            4030L,
            "010000008786E1FE747F0545A0630B701F5E10A26C6BDBBDBEC4F34892B17795000000006670DC27B3D686DF3AFBADFD8888CE93276AD94E62881F660BF0255DF8682EE239C0BF4FFFFF001D0ACDFE510101000000010000000000000000000000000000000000000000000000000000000000000000FFFFFFFF1F0439C0BF4F0104172F503253482F49636549726F6E2D51432D6D696E65722FFFFFFFFF0100F2052A010000002321039A68569864A29872632DA58879C4A820029C2C3A5B6869969A3D87C7D6EDF0DDAC00000000",
            difficultyCalculatorContext
        );
        _loadBlock(
            4031L,
            "01000000B06831147C84E7835E0AAAE59A9E56476896CF09F2493BE64928074B000000006096CD1532E9EDF3E52045A2F5C220D5782630C77A55C0EEF17C5F87B701774939C0BF4FFFFF001D186FBB4E0101000000010000000000000000000000000000000000000000000000000000000000000000FFFFFFFF1F0439C0BF4F011C172F503253482F49636549726F6E2D51432D6D696E65722FFFFFFFFF0100F2052A010000002321029CC239EB9233C0C6B27EDE841BD4ACDE244068C28F71E831A34FD37B79994604AC00000000",
            difficultyCalculatorContext
        );
        final BlockHeader blockHeader = _loadBlock(
            4032L,
            "01000000C7B49F0DAB7CF7D20AC9F654C7E9B2E12921D7F8CC669199FCCF9C2E00000000AD205DC405B584A3FC8076299DFCFA2B52D436B40E7FF75E8B0E1499EDCFB03C3AC0BF4FC0FF3F1C1FD27A3E0101000000010000000000000000000000000000000000000000000000000000000000000000FFFFFFFF1F043AC0BF4F0111172F503253482F49636549726F6E2D51432D6D696E65722FFFFFFFFF0100F2052A0100000023210224E7FF9142B9B0945910B16FA3139DC9177FEC58EF6749D50FA5F437830120C3AC00000000",
            difficultyCalculatorContext
        );

        final DifficultyCalculator difficultyCalculator = new TestNetDifficultyCalculator(difficultyCalculatorContext);

        // Action
        final Difficulty difficulty = difficultyCalculator.calculateRequiredDifficulty(4032L);

        // Assert
        Assert.assertEquals(blockHeader.getDifficulty(), difficulty);
        Assert.assertTrue(difficulty.isSatisfiedBy(blockHeader.getHash()));
    }

    @Test
    public void should_calculate_difficulty_for_block_00000000DB623A1752143F2F805C4527573570D9B4CA0A3CFE371E703AC429AA() {
        // Setup
        final UpgradeSchedule upgradeSchedule = new TestNetUpgradeSchedule();
        final FakeDifficultyCalculatorContext difficultyCalculatorContext = new FakeDifficultyCalculatorContext(upgradeSchedule);

        _loadBlock(
            4032L,
            "01000000C7B49F0DAB7CF7D20AC9F654C7E9B2E12921D7F8CC669199FCCF9C2E00000000AD205DC405B584A3FC8076299DFCFA2B52D436B40E7FF75E8B0E1499EDCFB03C3AC0BF4FC0FF3F1C1FD27A3E0101000000010000000000000000000000000000000000000000000000000000000000000000FFFFFFFF1F043AC0BF4F0111172F503253482F49636549726F6E2D51432D6D696E65722FFFFFFFFF0100F2052A0100000023210224E7FF9142B9B0945910B16FA3139DC9177FEC58EF6749D50FA5F437830120C3AC00000000",
            difficultyCalculatorContext
        );
        final BlockHeader blockHeader = _loadBlock(
            4033L,
            IoUtil.getResource("/blocks/00000000DB623A1752143F2F805C4527573570D9B4CA0A3CFE371E703AC429AA"),
            difficultyCalculatorContext
        );

        final DifficultyCalculator difficultyCalculator = new TestNetDifficultyCalculator(difficultyCalculatorContext);

        // Action
        final Difficulty difficulty = difficultyCalculator.calculateRequiredDifficulty(4033L);

        // Assert
        Assert.assertEquals(blockHeader.getDifficulty(), difficulty);
        Assert.assertTrue(difficulty.isSatisfiedBy(blockHeader.getHash()));
    }

    @Test
    public void should_calculate_difficulty_for_block_0000000030B3DC00BFD9E8AE426ECF36BD6D25F28D83B53AC9A7FDAF886A9CE8() {
        // Setup
        final UpgradeSchedule upgradeSchedule = new TestNetUpgradeSchedule();
        final FakeDifficultyCalculatorContext difficultyCalculatorContext = new FakeDifficultyCalculatorContext(upgradeSchedule);

        _loadBlock(
            4031L,
            "01000000B06831147C84E7835E0AAAE59A9E56476896CF09F2493BE64928074B000000006096CD1532E9EDF3E52045A2F5C220D5782630C77A55C0EEF17C5F87B701774939C0BF4FFFFF001D186FBB4E0101000000010000000000000000000000000000000000000000000000000000000000000000FFFFFFFF1F0439C0BF4F011C172F503253482F49636549726F6E2D51432D6D696E65722FFFFFFFFF0100F2052A010000002321029CC239EB9233C0C6B27EDE841BD4ACDE244068C28F71E831A34FD37B79994604AC00000000",
            difficultyCalculatorContext
        );
        _loadBlock(
            4032L,
            "01000000C7B49F0DAB7CF7D20AC9F654C7E9B2E12921D7F8CC669199FCCF9C2E00000000AD205DC405B584A3FC8076299DFCFA2B52D436B40E7FF75E8B0E1499EDCFB03C3AC0BF4FC0FF3F1C1FD27A3E0101000000010000000000000000000000000000000000000000000000000000000000000000FFFFFFFF1F043AC0BF4F0111172F503253482F49636549726F6E2D51432D6D696E65722FFFFFFFFF0100F2052A0100000023210224E7FF9142B9B0945910B16FA3139DC9177FEC58EF6749D50FA5F437830120C3AC00000000",
            difficultyCalculatorContext
        );
        _loadBlock(
            4033L,
            IoUtil.getResource("/blocks/00000000DB623A1752143F2F805C4527573570D9B4CA0A3CFE371E703AC429AA"),
            difficultyCalculatorContext
        );

        { // Simulate the real-scenario of perpetual 20-minute reset-blocks.
            final BlockHeaderInflater blockHeaderInflater = new BlockHeaderInflater();
            final HashMap<Long, BlockHeader> blockHeaders = difficultyCalculatorContext.getBlockHeaders();
            final HashMap<Long, MedianBlockTime> medianBlockTimes = difficultyCalculatorContext.getMedianBlockTimes();

            int timestampHackCount = 0;
            for (long blockHeight = 4207L; blockHeight >= 4034L; --blockHeight) {
                final MutableBlockHeader blockHeader = blockHeaderInflater.fromBytes(ByteArray.fromHexString("01000000297AF2563B650F6E99173E5D2AF552E5863566CC7367B55E601CDE60000000006FF5375F78FA999974677FB5FBB69C9C5961B120A81845B4AA97A0E159C00CE939F5C24FFFFF001D0E0084190101000000010000000000000000000000000000000000000000000000000000000000000000FFFFFFFF3F0439F5C24F0106372F503253482F204C6561677565206F66206173746F6E697368696E676C79206C75636B792061626163757320656E746875736961737473FFFFFFFF0100F2052A01000000232102FE9F54A1A09D1BB7BFE441650E52FFF7E4A0994005105784304240C11F3CAB0EAC00000000"));
                blockHeader.setTimestamp(blockHeader.getTimestamp() - (20L * 60L * timestampHackCount));
                blockHeaders.put(blockHeight, blockHeader);
                medianBlockTimes.put(blockHeight, MedianBlockTime.fromSeconds(MedianBlockTime.GENESIS_BLOCK_TIMESTAMP));
                timestampHackCount += 1;
            }
        }

        _loadBlock(
            4208L,
            "01000000F8DCD822CD43CA2C0D9EA6A97E6C63692701E9FDF85B6012E1A438C300000000AB642CBF77DC28BFD12A5BB134AA8F753E95A33E94DEEF7DD7786305CCF18B52EAF9C24FFFFF001D148B509C0101000000010000000000000000000000000000000000000000000000000000000000000000FFFFFFFF3F04EAF9C24F0110372F503253482F204C6561677565206F66206173746F6E697368696E676C79206C75636B792061626163757320656E746875736961737473FFFFFFFF0100F2052A010000002321034700EE1C343333E1956C5FBEBBCB553F19F2E5315E0246BA47E0A01C38CD1D85AC00000000",
            difficultyCalculatorContext
        );
        final BlockHeader blockHeader = _loadBlock(
            4209L,
            "010000004E16CDE4321E79D2DAD4A5453C3DC6558F2A5102222057CA46C1F0EC00000000E11EEB749B186B07DF928AC97F76413E2336175B0EC0AE5E7A2B49092F51886120ECC24FC0FF3F1CA70357FD0101000000010000000000000000000000000000000000000000000000000000000000000000FFFFFFFF0E0420ECC24F010F062F503253482FFFFFFFFF0100F2052A01000000232103C853C388F26B6DC0E2B915BF74D02AB7F45AF12D4880D783965A84AFFB7E552AAC00000000",
            difficultyCalculatorContext
        );

        final DifficultyCalculator difficultyCalculator = new TestNetDifficultyCalculator(difficultyCalculatorContext);

        // Action
        final Difficulty difficulty = difficultyCalculator.calculateRequiredDifficulty(4209L);

        // Assert
        Assert.assertEquals(blockHeader.getDifficulty(), difficulty);
        Assert.assertTrue(difficulty.isSatisfiedBy(blockHeader.getHash()));

        for (final BlockHeader unusedBlockHeader : difficultyCalculatorContext.getUnusedBlocks()) {
            System.out.println("Unused: " + unusedBlockHeader.getHash().toString().toLowerCase());
        }
    }

    @Test
    public void should_calculate_difficulty_for_block_0000000000085B97C5648887C2A49B1273FF352ABAD85A977E2F3C5405E5C1ED() {
        // Setup
        final UpgradeSchedule upgradeSchedule = new TestNetUpgradeSchedule();
        final FakeDifficultyCalculatorContext difficultyCalculatorContext = new FakeDifficultyCalculatorContext(upgradeSchedule);

        _loadBlock(
            478555L,
            "03000000FE302E5929A113C9E59D2899E2A258DADF91A35870EED2D4C546010000000000AE1A259463C903F94ECD98DDEA4DFC7A48D8C2EECA8DF97E31E566480AD858C591B9805570AD111BCDE1DD520101000000010000000000000000000000000000000000000000000000000000000000000000FFFFFFFF36035B4D0707062F503253482F04F7A180550439242D2D0C567F946181090000000000000A636B706F6F6C0B2F4576696C20506F6F6C2FFFFFFFFF01807C814A000000001976A914876FBB82EC05CAA6AF7A3B5E5A983AAE6C6CC6D688AC00000000",
            difficultyCalculatorContext
        );
        _loadBlock(
            478560L,
            "03000000B49634BAC1569759B6886D1201BDD781490A9B2E553BECBAC551090000000000E62022CF68D70103E6B8120673F5C747BA3F795542A52BDA585767822728D19194B9805570AD111B88B334410101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff2f03604d0707062f503253482ffe10a28055fe66710e000963676d696e6572343208e400000000000000046b69776900ffffffff01807c814a000000001976a91496621bc1c9d1e5a1293e401519365de820792bbc88ac00000000",
            difficultyCalculatorContext
        );
        _loadBlock(
            478561L,
            "03000000CAB49FC808BC471778A06BADCC7003A3F8C57B5447166E3AEE82010000000000EA1624D08EDA2AC1FBF21E15D0370479599D9CB3458AA3177B1DC087DEFEC18F45BE8055FFFF001D10A9B8A80101000000010000000000000000000000000000000000000000000000000000000000000000FFFFFFFF0403614D07FFFFFFFF04807C814A000000001976A914FDB9FB622B0DB8D9121475A983288A0876F4DE4888AC0000000000000000226A200000000000000000000000000000000000000000000000000000FFFF0000000000000000000000001B6A1976A914FDB9FB622B0DB8D9121475A983288A0876F4DE4888AC0000000000000000326A30EA7C240AAA1805001CE2A0725FF361F3466A45FE65198D1822CA3015DCD6B6C50167AC467D61227132E1CCCB1840340000000000",
            difficultyCalculatorContext
        );
        final BlockHeader blockHeader = _loadBlock(
            478562L,
            "03000000454535B89EDE5C9A9CD528E704FF3E20BB44136D7B333710EF15E75C0000000040E2025852C17AF4F20139CFBC8108F957526367BEF411B0FE22861AADE12EB2A6B9805570AD111B2FF594760101000000010000000000000000000000000000000000000000000000000000000000000000FFFFFFFF0403624D07FFFFFFFF04807C814A000000001976A914FDB9FB622B0DB8D9121475A983288A0876F4DE4888AC0000000000000000226A200000000000000000000000000000000000000000000000000000FFFF0000000000000000000000001B6A1976A914FDB9FB622B0DB8D9121475A983288A0876F4DE4888AC0000000000000000326A30B3B3A40AAA1805001CE2A0725FF361F3466A45FE65198D1822CA3015DCD6B6C50167AC467D61227132E1CCCB6341340000000000",
            difficultyCalculatorContext
        );

        final DifficultyCalculator difficultyCalculator = new TestNetDifficultyCalculator(difficultyCalculatorContext);

        // Action
        final Difficulty difficulty = difficultyCalculator.calculateRequiredDifficulty(478562L);

        // Assert
        Assert.assertEquals(blockHeader.getDifficulty(), difficulty);
        Assert.assertTrue(difficulty.isSatisfiedBy(blockHeader.getHash()));
    }

    // Emergency DAA
    @Test
    public void should_calculate_difficulty_for_block_000000000000009264EC88DEDB3AA88ECB22F8746A25AF2798E4DAB3B8F69A7E() {
        // Setup
        final UpgradeSchedule upgradeSchedule = new TestNetUpgradeSchedule();
        final FakeDifficultyCalculatorContext difficultyCalculatorContext = new FakeDifficultyCalculatorContext(upgradeSchedule);

        _loadBlock(
                1155870L,
                "00000020E915F65CA0788627489F5099EFFDE96C7904FD405112777DBF010000000000003C8DEC575A87FFB9C2EC1C4758BC993B798D7E6F830CBDCC702875D5ADCB1895E37480593975061ABA72EAA10101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff45031ea3110004e3748059044b4c9e380c59306259a96d0100000000000a636b706f6f6c212f6d696e65642062792077656564636f646572206d6f6c69206b656b636f696e2fffffffff02484ba609000000001976a91427f60a3b92e8a92149b18210457cc6bdc14057be88ac0000000000000000266a24aa21a9ed1c8f3bf052b49e09a63e75287a7b66e4ea9d5fb8a02f96a529538973334b065500000000",
                difficultyCalculatorContext
        );
        _loadBlock(
                1155872L,
                "000000208E7184F016BF4568D7864FA948E59CF596A82A8A90F974CDBE0500000000000014C7BE6D19F7BE2922BB2D0D61FD0A9A73241199EF3504DF224F2F22258402E3F67A80593975061A7AC760650101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff450320a3110004f67a805904bb65f7360c59306259c5100100000000000a636b706f6f6c212f6d696e65642062792077656564636f646572206d6f6c69206b656b636f696e2fffffffff024a818309000000001976a91427f60a3b92e8a92149b18210457cc6bdc14057be88ac0000000000000000266a24aa21a9eda9ea1eb7e392c6a3a211e7f4e9747b0e6ae727eca829f5540c8800766e43091400000000",
                difficultyCalculatorContext
        );
        _loadBlock(
                1155873L,
                "000000202F9FFD4181E8E9A6CE78197004EFB03E615EFF1F8ACF2267EB04000000000000F14F8DB561DF9A981E20FEA9BB8FE83926794F5E066FEAD892816E8D8B52B117A97F8059FFFF001D243C34900101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff230321a31100fea97f8059feb0c801000963676d696e6572343208010000000000000000ffffffff021c808d09000000001976a9144bfe90c8e6c6352c034b3f57d50a9a6e77a62a0788ac0000000000000000266a24aa21a9ed8d75c63225f9e25bc652c8eb0618179ccc5382d2b7ac4678ed3574a10ebefebb00000000",
                difficultyCalculatorContext
        );
        _loadBlock(
                1155874L,
                "000000207E02986D71F231C2AF30A1D04A12931C411756715BB7EF43B4843B9F00000000BE024FBE723A0418C0D213B8D33C321FC6AD84568567AC14039F8DE73D2C0B0C5A848059FFFF001D1ACBE1C10101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff230322a31100fe5a848059fe158707000963676d696e6572343208000000000000000000ffffffff02caed8a09000000001976a9144bfe90c8e6c6352c034b3f57d50a9a6e77a62a0788ac0000000000000000266a24aa21a9ed974257e6691991abd2f93bbce0caa9965cd8465323153ec11eb6eeec4d1ebdc400000000",
                difficultyCalculatorContext
        );
        _loadBlock(
                1155875L,
                "0000002092264ACC377804C50ABDF0C51B270229233CE16BABFC3CEA3F656CCE0000000092B34C2577E5C00F647A66D06F451CF3458F1D8F76A46DA6B7EDB656EFB1AFC10B898059FFFF001D1E5C53280101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff230323a31100fe0b898059fe34460c000963676d696e6572343208020000000000000000ffffffff02af618a09000000001976a9144bfe90c8e6c6352c034b3f57d50a9a6e77a62a0788ac0000000000000000266a24aa21a9edec73e11c8a90f3580742c489861a7f510c90e1f9b8e740a154d973c8ca0fd94900000000",
                difficultyCalculatorContext
        );
        _loadBlock(
                1155876L,
                "0200002038F1AE7F0EA8C1B589884C5FBD0B83721E3AB6759A4B897206857CF100000000F0A85830DEA9449B03F845D6B62EAC4D4679508B2FA91D1ED6EE2D0406DEEEC958BD8059FFFF001D9A94B8ED0101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff170324a3110a506f6f6c06205980bd580000000069020000ffffffff01793cc80b000000001976a914b939fdc5c4dd28318fd80b4203dc43003c4353ec88ac00000000",
                difficultyCalculatorContext
        );
        final BlockHeader blockHeader = _loadBlock(
                1155877L,
                "02000020F5B885A2A40E9A0CF77ED39FBAC2D51538F47D2A58D93EF9FE380E00000000006E73BD24FEBCA354CBEF8985FDE9CC6F1EDB5C3D99F0BCBEFE94CC6CABED31F06CBD80593975061A2E7238EF0101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff170325a3110a506f6f6c06205980bd6c0000000035080000ffffffff01902f5009000000001976a914b939fdc5c4dd28318fd80b4203dc43003c4353ec88ac00000000",
                difficultyCalculatorContext
        );

        final DifficultyCalculator difficultyCalculator = new TestNetDifficultyCalculator(difficultyCalculatorContext);

        // Action
        final Difficulty difficulty = difficultyCalculator.calculateRequiredDifficulty(1155877L);

        // Assert
        Assert.assertEquals(blockHeader.getDifficulty(), difficulty);
        Assert.assertTrue(difficulty.isSatisfiedBy(blockHeader.getHash()));
    }

    // CW144 DAA
    @Test
    public void should_calculate_difficulty_for_block_000000000001ED8CB72E21F25CE520773808CE380562C4C0A6F67E9447063AD0() {
        // Setup
        final UpgradeSchedule upgradeSchedule = new TestNetUpgradeSchedule();
        final FakeDifficultyCalculatorContext difficultyCalculatorContext = new FakeDifficultyCalculatorContext(upgradeSchedule);

        _setChainWork(1188559L, "0000000000000000000000000000000000000000000000288037c139a7bd5225", difficultyCalculatorContext);
        _loadBlock(
                1188560L,
                "000000205CCD5308CCEFAE98EA306958137CA1BEA717A121B6155FD49F560F00000000008C634B367CA3F18FF1104F25B7F810299D2132D9AE41B388E454DDDEF28F66BDCBC9095AC02D231BF7BF327F0101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff3203d022120004fbc9095a043f2b4a260c0bae095ad7030000000000000a636b706f6f6c0e2f2046756e6e794d696e6572202fffffffff02902f5009000000001976a9147dbba1ae6001b2e52d0480af7c9e5710e0b62b3388ac0000000000000000266a24aa21a9ede2f61c3f71d1defd3fa999dfa36953755c690689799962b48bebd836974e8cf900000000",
                difficultyCalculatorContext,
                true
        );
        _loadBlock(
                1188561L,
                "0000002070504C4255C7EA88EB9F7EF0F04D53BBAEB14F5527FC963397891700000000008CCD0FF6D32B06AAFF26B015CAA441FB0C5600612760F517A154A35F8E27C85BDCC9095AC02D231B9389C2A60101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff3203d1221200040cca095a04ce3ef9290c0bae095af2170000000000000a636b706f6f6c0e2f2046756e6e794d696e6572202fffffffff02902f5009000000001976a9147dbba1ae6001b2e52d0480af7c9e5710e0b62b3388ac0000000000000000266a24aa21a9ede2f61c3f71d1defd3fa999dfa36953755c690689799962b48bebd836974e8cf900000000",
                difficultyCalculatorContext,
                true
        );
        _loadBlock(
                1188562L,
                "00000020DEB2A800929E9D3EC58ED913860A4F73AA35551C5D6667B596D5100000000000C320B604ABDA3F419E4B60270F7628F4BB0AB64F2349C306BE35EEDA49668552E0C9095AC02D231BD5E2B37B0101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff3203d22212000410ca095a04c764560d0c0bae095a48010000000000000a636b706f6f6c0e2f2046756e6e794d696e6572202fffffffff02902f5009000000001976a9147dbba1ae6001b2e52d0480af7c9e5710e0b62b3388ac0000000000000000266a24aa21a9ede2f61c3f71d1defd3fa999dfa36953755c690689799962b48bebd836974e8cf900000000",
                difficultyCalculatorContext,
                true
        );
        _setChainWork(1188703L, "000000000000000000000000000000000000000000000028803bc6a9d4671a84", difficultyCalculatorContext);
        _loadBlock(
                1188704L,
                "00000020CEA3F11F4C59B52BE6DED843E10245896B7C06DE756F13909052040000000000B9DD9A55E0941FB124BA42231586FEBC6EC02E115AF609D50F429421CF34C13BE6FB095A4806121B57E37B310101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff3203602312000416fc095a0498c72f240c8fd5095ab7080000000000000a636b706f6f6c0e2f2046756e6e794d696e6572202fffffffff02902f5009000000001976a9147dbba1ae6001b2e52d0480af7c9e5710e0b62b3388ac0000000000000000266a24aa21a9ede2f61c3f71d1defd3fa999dfa36953755c690689799962b48bebd836974e8cf900000000",
                difficultyCalculatorContext,
                true
        );
        _loadBlock(
                1188705L,
                "00000020BAEDAFDF14DFC30ED4C0CB9F3F78BAF3D806B5DFCAA0BB2F8DEB0C0000000000ED332D820FE8F66A4705100597CBF440F14733029CDD00B8CAE6A00AEDA08C2BEBFB095AA8E7111B36EF0C1E0101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff320361231200041bfc095a04afe430240c8fd5095aec070000000000000a636b706f6f6c0e2f2046756e6e794d696e6572202fffffffff02902f5009000000001976a9147dbba1ae6001b2e52d0480af7c9e5710e0b62b3388ac0000000000000000266a24aa21a9ede2f61c3f71d1defd3fa999dfa36953755c690689799962b48bebd836974e8cf900000000",
                difficultyCalculatorContext,
                true
        );
        _loadBlock(
                1188706L,
                "00000020C6ACF2571E4EE81FA695681A520C493B266AAD9A4EF9F72C9C5A0900000000000BAE561D49CF30BE4B18E29F0DCE1D34A334CBE647DC4A495CFAB750CC6B87F5F0FB095A06C9111B424E14210101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff3203622312000420fc095a042f3f32240c8fd5095a7f0a0000000000000a636b706f6f6c0e2f2046756e6e794d696e6572202fffffffff02902f5009000000001976a9147dbba1ae6001b2e52d0480af7c9e5710e0b62b3388ac0000000000000000266a24aa21a9ede2f61c3f71d1defd3fa999dfa36953755c690689799962b48bebd836974e8cf900000000",
                difficultyCalculatorContext,
                true
        );
        final BlockHeader blockHeader = _loadBlock(
                1188707L,
                "00000020F45332987963055F8291A9898892DEA249BC401EA5FB323C1B100B0000000000968D01C41D80BD3BD99BD2279C527AC6B07929AE74556F155532EC1900D9A5B5FAFB095A63AA111B8AF5CA5B0101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff320363231200042afc095a04c05829240c8fd5095aeb0d0000000000000a636b706f6f6c0e2f2046756e6e794d696e6572202fffffffff02902f5009000000001976a9147dbba1ae6001b2e52d0480af7c9e5710e0b62b3388ac0000000000000000266a24aa21a9ede2f61c3f71d1defd3fa999dfa36953755c690689799962b48bebd836974e8cf900000000",
                difficultyCalculatorContext,
                true
        );

        final DifficultyCalculator difficultyCalculator = new TestNetDifficultyCalculator(difficultyCalculatorContext);

        // Action
        final Difficulty difficulty = difficultyCalculator.calculateRequiredDifficulty(1188707L);

        // Assert
        Assert.assertEquals(blockHeader.getDifficulty(), difficulty);
        Assert.assertTrue(difficulty.isSatisfiedBy(blockHeader.getHash()));
    }

    // ASERT DAA
    @Test
    public void should_calculate_difficulty_for_block_000000000D2592BD8568191E895500B5A0576308313E9508A88E09CFA0011989() {
        // Setup
        final UpgradeSchedule upgradeSchedule = new TestNetUpgradeSchedule();
        final FakeDifficultyCalculatorContext difficultyCalculatorContext = new FakeDifficultyCalculatorContext(upgradeSchedule) {
            @Override
            public AsertReferenceBlock getAsertReferenceBlock() {
                return new AsertReferenceBlock(1421481L, 1605445400L, Difficulty.decode(ByteArray.wrap(HexUtil.hexStringToByteArray("1d00ffff"))));
            }
        };

        _loadBlock( // ASERT Reference Block
                1421481L,
                "0000002048E4919801564248B56C7452FA24EAECA06C1C96715B58C68FFFC20F000000000CF85F5BF9D829EAD650E17D598DAF0C6A5A18081346186DC150AEF4BD091731CD2BB15FFFFF001D2C9FE24A0101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0403a9b015ffffffff030e014804000000001976a914d39b00b07e0ebc72fea383830264fd33e6dc16aa88acba1660000000000017a914260617ebf668c9102f71ce24aba97fcaaf9c666a8700000000000000002a6a28fd11a42e8c0e12370270afaf4c3bfb1fdbee0298934f816d2dd800671ca6e945a07d8efb0500000000000000",
                1605442008L,
                difficultyCalculatorContext
        );
        _loadBlock(
                1426011L,
                "0000002042565789DD35D94BC1F3350CC3F6F3B7F014DEF48899118AAEEBCC9400000000E70B1DA5F6BBD2803DACF6DE432745521C6715E6500BFF69C25D0521A73F2255916EDA5FFFFF001D34CA5EA30101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff37035bc21500fe916eda5ffedc900b000963676d696e6572343208010000000000000013626974636f696e636173682e6e6574776f726b00ffffffff01c817a804000000001976a914158b5d181552c9f4f267c0de68aae4963043993988ac00000000",
                1608143671L,
                difficultyCalculatorContext
        );
        final BlockHeader blockHeader = _loadBlock(
                1426012L,
                "0000002043A306A575AB1A7D8F72AD49153B0EAC238138E44F6D326C98B33CCE000000007418CEC961F1E248DF4075711D16A9202685C18C4919618C00DAFA44D1485766936EDA5FDCAB131CD290367D0101000000010000000000000000000000000000000000000000000000000000000000000000ffffffff37035cc21500fe936eda5ffe8bff03000963676d696e65723432080d0000000000000013626974636f696e636173682e6e6574776f726b00ffffffff01c817a804000000001976a914158b5d181552c9f4f267c0de68aae4963043993988ac00000000",
                1608145362L,
                difficultyCalculatorContext
        );

        final DifficultyCalculator difficultyCalculator = new TestNetDifficultyCalculator(difficultyCalculatorContext);

        // Action
        final Difficulty difficulty = difficultyCalculator.calculateRequiredDifficulty(1426012L);

        // Assert
        Assert.assertEquals(blockHeader.getDifficulty(), difficulty);
        Assert.assertTrue(difficulty.isSatisfiedBy(blockHeader.getHash()));
    }

    /**
     * To be enabled temporarily to convert JSON from https://testnet2.imaginary.cash/ to a block header,
     * so that block data can be used in the tests above.  The block data is not always valid, as it assumes the only
     * transaction is the coinbase, though that shouldn't be a problem for these tests.
     */
    @Test
    public void _jsonToSimplifiedBlockData() {
        final String blockJsonString = "{\n" +
                "    \"hash\": \"00000000062c7f32591d883c99fc89ebe74a83287c0f2b7ffeef72e62217d40b\",\n" +
                "    \"confirmations\": 4855,\n" +
                "    \"size\": 253,\n" +
                "    \"height\": 1421481,\n" +
                "    \"version\": 536870912,\n" +
                "    \"versionHex\": \"20000000\",\n" +
                "    \"merkleroot\": \"311709bdf4ae50c16d18461308185a6a0caf8d597de150d6ea29d8f95b5ff80c\",\n" +
                "    \"tx\": \"See 'Transaction IDs'\",\n" +
                "    \"time\": 1605446605,\n" +
                "    \"mediantime\": 1605442008,\n" +
                "    \"nonce\": 1256365868,\n" +
                "    \"bits\": \"1d00ffff\",\n" +
                "    \"difficulty\": 1,\n" +
                "    \"chainwork\": \"00000000000000000000000000000000000000000000006e7acfacf80f43ec25\",\n" +
                "    \"nTx\": 1,\n" +
                "    \"previousblockhash\": \"000000000fc2ff8fc6585b71961c6ca0ecea24fa52746cb5484256019891e448\",\n" +
                "    \"nextblockhash\": \"0000000023e0680a8a062b3cc289a4a341124ce7fcb6340ede207e194d73b60a\",\n" +
                "    \"coinbaseTx\": {\n" +
                "        \"txid\": \"311709bdf4ae50c16d18461308185a6a0caf8d597de150d6ea29d8f95b5ff80c\",\n" +
                "        \"hash\": \"311709bdf4ae50c16d18461308185a6a0caf8d597de150d6ea29d8f95b5ff80c\",\n" +
                "        \"version\": 1,\n" +
                "        \"size\": 172,\n" +
                "        \"locktime\": 0,\n" +
                "        \"vin\": [\n" +
                "            {\n" +
                "                \"coinbase\": \"03a9b015\",\n" +
                "                \"sequence\": 4294967295\n" +
                "            }\n" +
                "        ],\n" +
                "        \"vout\": [\n" +
                "            {\n" +
                "                \"value\": 0.71827726,\n" +
                "                \"n\": 0,\n" +
                "                \"scriptPubKey\": {\n" +
                "                    \"asm\": \"OP_DUP OP_HASH160 d39b00b07e0ebc72fea383830264fd33e6dc16aa OP_EQUALVERIFY OP_CHECKSIG\",\n" +
                "                    \"hex\": \"76a914d39b00b07e0ebc72fea383830264fd33e6dc16aa88ac\",\n" +
                "                    \"reqSigs\": 1,\n" +
                "                    \"type\": \"pubkeyhash\",\n" +
                "                    \"addresses\": [\n" +
                "                        \"bchtest:qrfekq9s0c8tcuh75wpcxqnyl5e7dhqk4gmw07xth0\"\n" +
                "                    ]\n" +
                "                }\n" +
                "            },\n" +
                "            {\n" +
                "                \"value\": 0.06297274,\n" +
                "                \"n\": 1,\n" +
                "                \"scriptPubKey\": {\n" +
                "                    \"asm\": \"OP_HASH160 260617ebf668c9102f71ce24aba97fcaaf9c666a OP_EQUAL\",\n" +
                "                    \"hex\": \"a914260617ebf668c9102f71ce24aba97fcaaf9c666a87\",\n" +
                "                    \"reqSigs\": 1,\n" +
                "                    \"type\": \"scripthash\",\n" +
                "                    \"addresses\": [\n" +
                "                        \"bchtest:pqnqv9lt7e5vjyp0w88zf2af0l92l8rxdghdzfvj4e\"\n" +
                "                    ]\n" +
                "                }\n" +
                "            },\n" +
                "            {\n" +
                "                \"value\": 0,\n" +
                "                \"n\": 2,\n" +
                "                \"scriptPubKey\": {\n" +
                "                    \"asm\": \"OP_RETURN fd11a42e8c0e12370270afaf4c3bfb1fdbee0298934f816d2dd800671ca6e945a07d8efb05000000\",\n" +
                "                    \"hex\": \"6a28fd11a42e8c0e12370270afaf4c3bfb1fdbee0298934f816d2dd800671ca6e945a07d8efb05000000\",\n" +
                "                    \"type\": \"nulldata\"\n" +
                "                }\n" +
                "            }\n" +
                "        ],\n" +
                "        \"hex\": \"01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff0403a9b015ffffffff030e014804000000001976a914d39b00b07e0ebc72fea383830264fd33e6dc16aa88acba1660000000000017a914260617ebf668c9102f71ce24aba97fcaaf9c666a8700000000000000002a6a28fd11a42e8c0e12370270afaf4c3bfb1fdbee0298934f816d2dd800671ca6e945a07d8efb0500000000000000\",\n" +
                "        \"blockhash\": \"00000000062c7f32591d883c99fc89ebe74a83287c0f2b7ffeef72e62217d40b\",\n" +
                "        \"confirmations\": 4855,\n" +
                "        \"time\": 1605446605,\n" +
                "        \"blocktime\": 1605446605\n" +
                "    },\n" +
                "    \"totalFees\": \"0\",\n" +
                "    \"subsidy\": \"0.78125\",\n" +
                "    \"miner\": []\n" +
                "}"; // <-- paste block JSON here and run test
        final Json json = Json.parse(blockJsonString);

        final Long blockHeight = json.getLong("height");
        final Long blockTimestamp = json.getLong("time");

        final MutableBlockHeader blockHeader = new MutableBlockHeader();
        blockHeader.setVersion(json.getLong("version"));
        blockHeader.setTimestamp(blockTimestamp);
        blockHeader.setNonce(json.getLong("nonce"));

        final ByteArray difficultyBytes = ByteArray.fromHexString(json.getString("bits"));
        final Difficulty difficulty = Difficulty.decode(difficultyBytes);
        blockHeader.setDifficulty(difficulty);

        final MerkleRoot merkleRoot = ImmutableMerkleRoot.fromHexString(json.getString("merkleroot"));
        blockHeader.setMerkleRoot(merkleRoot);

        final Sha256Hash previousBlockHash = Sha256Hash.fromHexString(json.getString("previousblockhash"));
        blockHeader.setPreviousBlockHash(previousBlockHash);

        final Sha256Hash expectedBlockHash = Sha256Hash.fromHexString(json.getString("hash"));

        final Sha256Hash blockHash = blockHeader.getHash();
        Assert.assertEquals(expectedBlockHash, blockHash);

        // BlockHeader
        final BlockHeaderDeflater blockHeaderDeflater = new BlockHeaderDeflater();
        String blockDataHex = blockHeaderDeflater.toBytes(blockHeader).toString();
        blockDataHex += "01" + json.get("coinbaseTx").getString("hex");
        System.out.println("BlockData: " + blockDataHex);
    }
}
