package com.softwareverde.bitcoin.context.core;

import com.softwareverde.bitcoin.server.module.node.database.DatabaseManagerFactory;
import com.softwareverde.bitcoin.server.module.node.manager.BitcoinNodeManager;
import com.softwareverde.bitcoin.server.module.node.sync.BlockHeaderDownloader;
import com.softwareverde.concurrent.pool.ThreadPool;
import com.softwareverde.network.time.VolatileNetworkTime;
import com.softwareverde.util.type.time.SystemTime;

public class BlockHeaderDownloaderContext implements BlockHeaderDownloader.Context {
    protected final BitcoinNodeManager _nodeManager;
    protected final DatabaseManagerFactory _databaseManagerFactory;
    protected final VolatileNetworkTime _networkTime;
    protected final SystemTime _systemTime;
    protected final ThreadPool _threadPool;

    public BlockHeaderDownloaderContext(final BitcoinNodeManager nodeManager, final DatabaseManagerFactory databaseManagerFactory, final VolatileNetworkTime networkTime, final SystemTime systemTime, final ThreadPool threadPool) {
        _nodeManager = nodeManager;
        _databaseManagerFactory = databaseManagerFactory;
        _networkTime = networkTime;
        _systemTime = systemTime;
        _threadPool = threadPool;
    }

    @Override
    public DatabaseManagerFactory getDatabaseManagerFactory() {
        return _databaseManagerFactory;
    }

    @Override
    public VolatileNetworkTime getNetworkTime() {
        return _networkTime;
    }

    @Override
    public BitcoinNodeManager getBitcoinNodeManager() {
        return _nodeManager;
    }

    @Override
    public SystemTime getSystemTime() {
        return _systemTime;
    }

    @Override
    public ThreadPool getThreadPool() {
        return _threadPool;
    }
}
