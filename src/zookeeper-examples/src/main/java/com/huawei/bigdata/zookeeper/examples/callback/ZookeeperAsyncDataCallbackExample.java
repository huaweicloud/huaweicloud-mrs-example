package com.huawei.bigdata.zookeeper.examples.callback;

import com.huawei.bigdata.zookeeper.examples.handler.ZookeeperCallbackHandlerExample;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

/**
 * This callback is used to retrieve the data and stat of the node.
 */
public class ZookeeperAsyncDataCallbackExample implements
        AsyncCallback.DataCallback
{
    private static final Logger LOG = Logger.getLogger(ZookeeperAsyncDataCallbackExample.class.getName());

    /**
     * Process the result of asynchronous calls.
     * <p/>
     * On success, rc is {@link Code#OK}.
     * <p/>
     * On failure, rc is set to the corresponding failure code in
     * {@link KeeperException}.
     * <ul>
     * <li>
     * {@link Code#NONODE} - The node on
     * given path doesn't exist for some API calls.</li>
     * </ul>
     *
     * @param rc The return code or the result of the call.
     * @param path The path that we passed to asynchronous calls.
     * @param ctx Whatever context object that we passed to asynchronous calls.
     * @param data The {@link org.apache.zookeeper.server.DataNode#data} of the
     *        node.
     * @param stat {@link Stat} object of the node on
     *        given path.
     */
    @Override
    public void processResult(int rc, String path, Object ctx, byte data[],
            Stat stat)
    {
        try
        {
            if (ctx.getClass()
                    .equals(Class
                            .forName("com.huawei.bigdata.zookeeper.examples.handler.ZookeeperCallbackHandlerExample")))
            {
                ZookeeperCallbackHandlerExample handler = (ZookeeperCallbackHandlerExample) ctx;
                if (rc == Code.Ok)
                {
                    handler.handle("reveive async message: " + path + " : "
                            + new String(data));
                }
                else
                {
                    Code code = Code.get(rc);
                    handler.handle("reveive async message: "
                            + KeeperException.create(code).getMessage());
                }
            }
            else
            {
                LOG.info("Error occured when handle asynchronous getdata calls");
            }
        }
        catch (ClassNotFoundException e)
        {
            LOG.info("Error occured when handle asynchronous getdata calls");
        }

    }
}
