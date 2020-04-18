package com.huawei.bigdata.zookeeper.examples.callback;

import com.huawei.bigdata.zookeeper.examples.handler.ZookeeperCallbackHandlerExample;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;

/**
 * This callback doesn't retrieve anything from the node. It is useful for some
 * APIs that doesn't want anything sent back, e.g.
 * {@link org.apache.zookeeper.ZooKeeper#sync(String, VoidCallback, Object)}
 * .
 */
public class ZookeeperAsyncVoidCallbackExample implements
        AsyncCallback.VoidCallback
{
    private static final Logger LOG = Logger.getLogger(ZookeeperAsyncVoidCallbackExample.class.getName());

    /**
     * Process the result of the asynchronous call.
     * <p/>
     * On success, rc is {@link Code#OK}.
     * <p/>
     * On failure, rc is set to the corresponding failure code in
     * {@link KeeperException}.
     * <ul>
     * <li>
     * {@link Code#NONODE} - The node on
     * given path doesn't exist for some API calls.</li>
     * <li>
     * {@link Code#BADVERSION} - The given
     * version doesn't match the node's version for some API calls.</li>
     * <li>
     * {@link Code#NOTEMPTY} - the node has
     * children and some API calls cannnot succeed, e.g.
     * {@link org.apache.zookeeper.ZooKeeper#delete(String, int, VoidCallback, Object)}
     * .</li>
     * </ul>
     *
     * @param rc The return code or the result of the call.
     * @param path The path that we passed to asynchronous calls.
     * @param ctx Whatever context object that we passed to asynchronous calls.
     */
    @Override
    public void processResult(int rc, String path, Object ctx)
    {
        try
        {
            if (ctx.getClass()
                    .equals(Class
                            .forName("com.huawei.bigdata.zookeeper.examples.handler.ZookeeperCallbackHandlerExample")))
            {
                ZookeeperCallbackHandlerExample handler = (ZookeeperCallbackHandlerExample) ctx;
                Code code = Code.get(rc);
                handler.handle("reveive async message: " + path + " : " + code);
            }
            else
            {
                LOG.info("Error occured when handle asynchronous void calls");
            }
        }
        catch (ClassNotFoundException e)
        {
            LOG.info("Error occured when handle asynchronous void calls");
        }
    }
}
