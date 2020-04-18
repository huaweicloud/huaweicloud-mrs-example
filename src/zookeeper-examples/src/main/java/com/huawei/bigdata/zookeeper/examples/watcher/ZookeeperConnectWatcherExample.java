package com.huawei.bigdata.zookeeper.examples.watcher;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

import java.util.concurrent.CountDownLatch;

public class ZookeeperConnectWatcherExample implements Watcher
{
    private static final Logger LOG = Logger.getLogger(ZookeeperConnectWatcherExample.class.getName());

    private CountDownLatch countDownLatch;

    public ZookeeperConnectWatcherExample(CountDownLatch countDownLatch)
    {
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void process(WatchedEvent watchedEvent)
    {
        if (watchedEvent.getType() == EventType.None)
        {
            KeeperState state = watchedEvent.getState();
            if (state == KeeperState.SyncConnected)
            {
                LOG.info("Connected");
                countDownLatch.countDown();

            }
            else if (state == KeeperState.Disconnected)
            {
                LOG.info("Disconnected");

            }
            else if (state == KeeperState.SaslAuthenticated)
            {
                LOG.info("SaslAuthenticated");
            }

            else if (state == KeeperState.AuthFailed)
            {
                LOG.info("AuthFailed");
            }
            else if (state == KeeperState.Expired)
            {
                LOG.info("Expired.");
            }
            else
            {
                LOG.info(state);
            }
        }
        else
        {
            LOG.info("watchedEvent.getType()="
                    + watchedEvent.getType().toString());
        }

    }
}
