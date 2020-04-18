package com.huawei.bigdata.zookeeper.examples.handler;

import org.apache.log4j.Logger;

/**
 * This class can be used to handle asynchronous calls
 */
public class ZookeeperCallbackHandlerExample
{
    private static final Logger LOG = Logger.getLogger(ZookeeperCallbackHandlerExample.class.getName());

    /**
     * Handle the str Just print it
     * @param str the content to be print
     */
    public void handle(String str)
    {
        LOG.info(str);
    }
}
