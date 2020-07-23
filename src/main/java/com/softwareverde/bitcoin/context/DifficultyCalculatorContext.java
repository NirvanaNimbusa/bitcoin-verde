package com.softwareverde.bitcoin.context;

import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.difficulty.work.ChainWork;
import com.softwareverde.bitcoin.chain.time.MedianBlockTime;

public interface DifficultyCalculatorContext {
    BlockHeader getBlockHeader(Long blockHeight);
    MedianBlockTime getMedianBlockTime(Long blockHeight);
    ChainWork getChainWork(Long blockHeight);
}
