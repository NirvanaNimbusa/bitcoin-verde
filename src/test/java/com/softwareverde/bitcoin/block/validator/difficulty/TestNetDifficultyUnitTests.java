package com.softwareverde.bitcoin.block.validator.difficulty;

import com.softwareverde.bitcoin.bip.CoreUpgradeSchedule;
import com.softwareverde.bitcoin.bip.TestNetUpgradeSchedule;
import com.softwareverde.bitcoin.bip.UpgradeSchedule;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.BlockHeaderInflater;
import com.softwareverde.bitcoin.block.header.MutableBlockHeader;
import com.softwareverde.bitcoin.block.header.difficulty.Difficulty;
import com.softwareverde.bitcoin.chain.time.MedianBlockTime;
import com.softwareverde.bitcoin.test.UnitTest;
import com.softwareverde.bitcoin.test.fake.FakeDifficultyCalculatorContext;
import com.softwareverde.bitcoin.util.IoUtil;
import com.softwareverde.constable.bytearray.ByteArray;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class TestNetDifficultyUnitTests extends UnitTest {

    protected BlockHeader _loadBlock(final Long blockHeight, final String blockData, final FakeDifficultyCalculatorContext context) {
        final BlockHeaderInflater blockHeaderInflater = new BlockHeaderInflater();
        final HashMap<Long, BlockHeader> blockHeaders = context.getBlockHeaders();
        final HashMap<Long, MedianBlockTime> medianBlockTimes = context.getMedianBlockTimes();

        final BlockHeader blockHeader = blockHeaderInflater.fromBytes(ByteArray.fromHexString(blockData));

        blockHeaders.put(blockHeight, blockHeader);
        medianBlockTimes.put(blockHeight, MedianBlockTime.fromSeconds(MedianBlockTime.GENESIS_BLOCK_TIMESTAMP));

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
}