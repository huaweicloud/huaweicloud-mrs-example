package com.huawei.bigdata.zookeeper.examples.watcher;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * ZookeeperWatcherExample is a simple watcher ZookeeperWatcherExample is an
 * example of watcher, it just get the event and print
 *
 */
public class ZookeeperWatcherExample implements Watcher
{
    private static final Logger LOG = Logger.getLogger(ZookeeperWatcherExample.class.getName());

    /**
     * This method process the event When receive the even, and print it
     * @param event The <code>WatchedEvent</code> received
     */
    @Override
    public void process(WatchedEvent event)
    {
        if (event.getType() == Event.EventType.NodeChildrenChanged)
        {
            LOG.info(event.getState().name()
                    + ", the children of the node was changed successfully.");
        }
        else if (event.getType() == Event.EventType.NodeCreated)
        {
            LOG.info(event.getState().name()
                    + ", the node was created successfully.");
        }
        else if (event.getType() == Event.EventType.NodeDataChanged)
        {
            LOG.info(event.getState().name()
                    + ", the data of the node was modified successfully.");
        }
        else if (event.getType() == Event.EventType.NodeDeleted)
        {
            LOG.info(event.getState().name()
                    + ", the node was deleted successfully.");
        }
        else
        {
            LOG.info("watchedEvent.getType()=" + event.getType().toString());
        }

    }
}
