package com.huawei.bigdata.zookeeper.examples.client;

import com.huawei.bigdata.zookeeper.examples.callback.ZookeeperAsyncACLCallbackExample;
import com.huawei.bigdata.zookeeper.examples.callback.ZookeeperAsyncChildrenCallbackExample;
import com.huawei.bigdata.zookeeper.examples.callback.ZookeeperAsyncDataCallbackExample;
import com.huawei.bigdata.zookeeper.examples.callback.ZookeeperAsyncStatCallbackExample;
import com.huawei.bigdata.zookeeper.examples.callback.ZookeeperAsyncStringCallbackExample;
import com.huawei.bigdata.zookeeper.examples.callback.ZookeeperAsyncVoidCallbackExample;
import com.huawei.bigdata.zookeeper.examples.handler.ZookeeperCallbackHandlerExample;
import com.huawei.bigdata.zookeeper.examples.parseconfig.ZKExampleConfigParser;
import com.huawei.bigdata.zookeeper.examples.watcher.ZookeeperConnectWatcherExample;
import com.huawei.bigdata.zookeeper.examples.watcher.ZookeeperWatcherExample;
import com.huawei.hadoop.security.LoginUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * This class shows how to use some common method This class simulate a scenes
 * only one zookeeper client
 */
public class ZookeeperSingleClientExample
{
    private static final Logger LOG = Logger.getLogger(ZookeeperSingleClientExample.class.getName());
    
    private static void init()
    {
        try {
            String saslClient = ZKExampleConfigParser.parseSaslClient();
            if ("true".equals(saslClient)) {
                String keytabPath = ZKExampleConfigParser.parseKeytabPath();
                String krb5path = ZKExampleConfigParser.parseKrb5Path();
                String username = ZKExampleConfigParser.parseUserName();
                String serverPrincipal = ZKExampleConfigParser.parseServerPrincipal();
                LoginUtil.setJaasFile(username, keytabPath);
                LoginUtil.setKrb5Config(krb5path);
                LoginUtil.setSaslClient(saslClient);
                LoginUtil.setZookeeperServerPrincipal(serverPrincipal);
            }
        }
        catch (IOException e)
        {
            LOG.error("init env failed, ", e);
        }
    }

    public static void main(String[] args) throws IOException
    {
        String confPath = null;
        if (args.length != 0)
        {
            confPath = args[0];
            LOG.info("the configuration path is " + confPath);
        }
        // load zk client properties.
        ZKExampleConfigParser.loadConfiguration(confPath);
        // init zk client env.
        init();
        String cxnString = ZKExampleConfigParser.parseCxnString();
        if (cxnString == null)
        {
            LOG.info("Error occured when parse connection String：" + cxnString);
        }
        int sessionTimeout = Integer.parseInt(ZKExampleConfigParser
                .parseTimeout());
        ZookeeperSingleClientExample singleclientexample = new ZookeeperSingleClientExample(
                cxnString, sessionTimeout);
        singleclientexample.run();
    }

    String cxnString;
    int sessionTimeout;
    public ZookeeperSingleClientExample(String cxnString, int sessionTimeout)
    {
        this.cxnString = cxnString;
        this.sessionTimeout = sessionTimeout;
    }

    public void run()
    {
        if (cxnString == null)
        {
            LOG.info("Error occured when parse ip adress");
        }
        connect(cxnString);

        String path1 = "/syncnode";
        String path2 = "/asyncnode";
        byte[] data = "testdata".getBytes();

        createSync(path1, data);
        createAsync(path2, data);

        LOG.info(getACLSync(path1));
        getACLAsync(path2);

        getchildrenAsyncWithoutWatcher("/");
        getchildrenAsyncWithWatcher("/");
        LOG.info(getchildrenSyncWithoutWatcher("/"));
        LOG.info(getchildrenSyncWithWatcher("/"));

        getDataAsyncWithoutWatcher(path2);
        getDataAsyncWithWatcher(path2);
        LOG.info(getDataSyncWithoutWatcher(path1) == null ? path1
                + " does not exist" : new String(getDataSyncWithoutWatcher(path1)));
        LOG.info(getDataSyncWithWatcher(path1) == null ? path1
                + " does not exist" : new String(getDataSyncWithWatcher(path1)));

        LOG.info(getVersion(path1));

        LOG.info(getStates() == null ? "state is null" : getStates().toString());

        LOG.info(getSessionId());
        LOG.info(getSessionTimeout());
        LOG.info(getSessionPasswd() == null ? "password is null" : new String(
                getSessionPasswd()));

        if (exitsSync(path1) != null)
        {
            deleteSync(path1);
        }
        deleteAsync(path2);
        close();
    }

    ZooKeeper zookeeper = null;

    ZookeeperWatcherExample watcher = new ZookeeperWatcherExample();

    ZookeeperAsyncStringCallbackExample stringCallback = new ZookeeperAsyncStringCallbackExample();

    ZookeeperAsyncVoidCallbackExample voidCallback = new ZookeeperAsyncVoidCallbackExample();

    ZookeeperAsyncStatCallbackExample statCallback = new ZookeeperAsyncStatCallbackExample();

    ZookeeperAsyncACLCallbackExample aclCallback = new ZookeeperAsyncACLCallbackExample();

    ZookeeperAsyncChildrenCallbackExample childrenCallback = new ZookeeperAsyncChildrenCallbackExample();

    ZookeeperAsyncDataCallbackExample dataCallback = new ZookeeperAsyncDataCallbackExample();

    ZookeeperCallbackHandlerExample handler = new ZookeeperCallbackHandlerExample();

    /**
     * Init a zookeeper client
     * @param CxnString The ip address of servers
     */
    public void connect(String CxnString)
    {
        try
        {
            CountDownLatch countDownLatch = new CountDownLatch(1);
            ZookeeperConnectWatcherExample watcher = new ZookeeperConnectWatcherExample(
                    countDownLatch);
            zookeeper = new ZooKeeper(CxnString, sessionTimeout, watcher);
            countDownLatch.await();
            LOG.info("Connected to " + zookeeper.toString());
        }
        catch (IOException | InterruptedException e)
        {
            LOG.info("Error occured when initiate ZooKeeper!");
        }
    }

    /**
     * Close the zookeeper client
     */
    public void close()
    {
        try
        {
            zookeeper.close();
        }
        catch (InterruptedException e)
        {
            LOG.info("Error occured when closing the client !");
        }
    }

    /**
     * Add an authinfo The access control mode is digest, authority information
     * is "guest:guest"
     */
    public void addAuthInfo()
    {
        zookeeper.addAuthInfo("digest", "guest:guest".getBytes());
    }

    /**
     * Create a List of ACL, which can be used in create znode Include two
     * scheme: Digest, World There are some other scheme: IP, Super. Not covered
     * here
     * @return acls The list of ACL
     */
    public List<ACL> createACL()
    {
        List<ACL> acls = new ArrayList<ACL>();
        Id id1 = null;
        try
        {
            id1 = new Id("digest",
                    DigestAuthenticationProvider.generateDigest("admin:admin"));
        }
        catch (NoSuchAlgorithmException e)
        {
            LOG.info("Error occered when generate Digest!");
        }
        ACL acl1 = new ACL(ZooDefs.Perms.ALL, id1);
        Id id2 = new Id("world", "anyone");
        ACL acl2 = new ACL(ZooDefs.Perms.READ, id2);
        acls.add(acl1);
        acls.add(acl2);
        return acls;
    }

    /**
     * Create a znode Synchronously
     * @param path The path of znode
     * @param data The data will be written to the znode
     */
    public void createSync(String path, byte[] data)
    {
        List<ACL> acls = createACL();
        try
        {
            zookeeper.create(path, data, acls, CreateMode.PERSISTENT);
        }
        catch (KeeperException e)
        {
            LOG.info("Error occured when create znode!");
        }
        catch (InterruptedException e)
        {
            LOG.info("Thread is interrupted when create znode!");
        }

    }

    /**
     * Create znode asynchronously Using
     * {@link ZookeeperAsyncStringCallbackExample} to process the result
     * @param path The path of znode
     * @param data The data will be written to the znode
     */
    public void createAsync(String path, byte[] data)
    {
        List<ACL> acls = createACL();
        zookeeper.create(path, data, acls, CreateMode.PERSISTENT,
                stringCallback, handler);
    }

    /**
     * Get znode version
     * @param path The path of znode
     * @return The version of znode if correct
     */
    public int getVersion(String path)
    {
        try
        {
            return zookeeper.exists(path, false).getVersion();
        }
        catch (KeeperException e)
        {
            LOG.info("Error occured when get version!");
        }
        catch (InterruptedException e)
        {
            LOG.info("Thread is interrupted when get version!");
        }
        return 0;
    }

    /**
     * Delete node synchronously
     * @param path The path of znode
     */
    public void deleteSync(String path)
    {
        try
        {
            zookeeper.delete(path, getVersion(path));
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        catch (KeeperException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Delete node asynchronously Using
     * {@link ZookeeperAsyncVoidCallbackExample} to process result
     * @param path The path of znode
     */
    public void deleteAsync(String path)
    {
        zookeeper.delete(path, getVersion(path), voidCallback, handler);
    }

    /**
     * Judge if znode exists synchronously without watcher
     * @param path The path of znode
     * @return the stat of znode if exists
     */
    public Stat exitsSync(String path)
    {
        try
        {
            return zookeeper.exists(path, false);
        }
        catch (KeeperException e)
        {
            e.printStackTrace();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Judge if znode exists synchronously with watcher Register
     * {@link ZookeeperWatcherExample} to watch the node
     * @param path The path of znode
     * @return the stat of znode if exists
     */
    public Stat exitsSyncWithWatcher(String path)
    {
        try
        {
            return zookeeper.exists(path, watcher);
        }
        catch (KeeperException e)
        {
            e.printStackTrace();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Judge if znode exists asynchronously without watcher Using
     * {@link ZookeeperAsyncStatCallbackExample} to process result
     * @param path The path of znode
     */
    public void exitsAsync(String path)
    {
        zookeeper.exists(path, false, statCallback, handler);
    }

    /**
     * judge if znode exists asynchronously with watcher Using
     * {@link ZookeeperAsyncStatCallbackExample} to process result Register
     * {@link ZookeeperWatcherExample} to watch the node
     * @param path The path of znode
     */
    public void exitsAsyncWithWatcher(String path)
    {
        zookeeper.exists(path, watcher, statCallback, handler);
    }

    /**
     * Get ACL of znode synchronously
     * @param path The path of znode
     * @return znode'ACL list
     */
    public List<ACL> getACLSync(String path)
    {
        try
        {
            return zookeeper.getACL(path, zookeeper.exists(path, false));
        }
        catch (KeeperException e)
        {
            LOG.info("Error occured when get ACL!");
        }
        catch (InterruptedException e)
        {
            LOG.info("Thread is interrupted when get ACL!");
        }
        return null;
    }

    /**
     * Get ACL of znode asynchronously using
     * {@link ZookeeperAsyncACLCallbackExample} to process result
     * @param path The path of znode
     */
    public void getACLAsync(String path)
    {
        try
        {
            zookeeper.getACL(path, zookeeper.exists(path, false), aclCallback,
                    handler);
        }
        catch (KeeperException e)
        {
            LOG.info("Error occured when get ACL!");
        }
        catch (InterruptedException e)
        {
            LOG.info("Thread is interrupted when get ACL!");
        }
    }

    /**
     * Get children of znode synchronously without watcher
     * @param path The path of znode
     * @return children list if correct
     */
    public List<String> getchildrenSyncWithoutWatcher(String path)
    {
        try
        {
            return zookeeper.getChildren(path, false);
        }
        catch (KeeperException e)
        {
            LOG.info("Error occured when get children!");
        }
        catch (InterruptedException e)
        {
            LOG.info("Thread is interrupted when get children!");
        }
        return null;
    }

    /**
     * Get children of znode synchronously with watcher Register
     * {@link ZookeeperWatcherExample} to watch the node
     * @param path The path of znode
     * @return children list if correct
     */
    public List<String> getchildrenSyncWithWatcher(String path)
    {
        try
        {
            return zookeeper.getChildren(path, watcher);
        }
        catch (KeeperException e)
        {
            LOG.info("Error occured when get children!");
        }
        catch (InterruptedException e)
        {
            LOG.info("Thread is interrupted when get children!");
        }
        return null;
    }

    /**
     * Get children of znode asynchronously without watcher Using
     * {@link ZookeeperAsyncChildrenCallbackExample} to process result
     * @param path The path of znode
     */
    public void getchildrenAsyncWithoutWatcher(String path)
    {
        zookeeper.getChildren(path, false, childrenCallback, handler);
    }

    /**
     * Get children of znode asynchronously with watcher Using
     * {@link ZookeeperAsyncChildrenCallbackExample} to process result Register
     * {@link ZookeeperWatcherExample} to watch the node
     * @param path The path of znode
     */
    public void getchildrenAsyncWithWatcher(String path)
    {
        zookeeper.getChildren(path, watcher, childrenCallback, handler);
    }

    /**
     * Get data of znode synchronously without watcher
     * @param path The path of znode
     * @return data of the znode
     */
    public byte[] getDataSyncWithoutWatcher(String path)
    {
        try
        {
            return zookeeper
                    .getData(path, false, zookeeper.exists(path, false));
        }
        catch (KeeperException e)
        {
            LOG.info("Error occured when get data!");
        }
        catch (InterruptedException e)
        {
            LOG.info("Thread is interrupted when get data!");
        }
        return null;
    }

    /**
     * Get data of znode asynchronously with watcher Register
     * {@link ZookeeperWatcherExample} to watch the node
     * @param path The path of znode
     * @return data of the znode
     */
    public byte[] getDataSyncWithWatcher(String path)
    {
        try
        {
            return zookeeper.getData(path, watcher,
                    zookeeper.exists(path, false));
        }
        catch (KeeperException e)
        {
            LOG.info("Error occured when get data!");
        }
        catch (InterruptedException e)
        {
            LOG.info("Thread is interrupted when get data!");
        }
        return null;
    }

    /**
     * Get data of znode asynchronously without watcher Using
     * {@link ZookeeperAsyncDataCallbackExample} to process result
     * @param path The path of znode
     */
    public void getDataAsyncWithoutWatcher(String path)
    {
        zookeeper.getData(path, false, dataCallback, handler);
    }

    /**
     * Get data of znode asynchronously with watcher Using
     * {@link ZookeeperAsyncDataCallbackExample} to process result Register
     * {@link ZookeeperWatcherExample} to watch the node
     * @param path The path of znode
     */
    public void getDataAsyncWithWatcher(String path)
    {
        zookeeper.getData(path, watcher, dataCallback, handler);
    }

    /**
     * Get session id
     * @return session id
     */
    public long getSessionId()
    {
        return zookeeper.getSessionId();
    }

    /**
     * Get session password
     * @return session password
     */
    public byte[] getSessionPasswd()
    {
        return zookeeper.getSessionPasswd();
    }

    /**
     * Get session timeout
     * @return session timeout
     */
    public int getSessionTimeout()
    {
        return zookeeper.getSessionTimeout();
    }

    /**
     * Get state of client
     * @return state of client
     */
    public States getStates()
    {
        return zookeeper.getState();
    }

    /**
     * Specify the default watcher for the connection
     */
    public void register()
    {
        zookeeper.register(watcher);
    }

    /**
     * Set the ACL for the node of the given path synchronously
     * @param path The path of znode
     * @param acl The ACL list set to the znode
     * @return stat of znode if correct
     */
    public Stat setACLSync(String path, List<ACL> acl)
    {
        try
        {
            return zookeeper.setACL(path, acl, getVersion(path));
        }
        catch (KeeperException e)
        {
            LOG.info("Error occured when set ACL!");
        }
        catch (InterruptedException e)
        {
            LOG.info("Thread is interrupted when set ACL!");
        }
        return null;
    }

    /**
     * Set the ACL for the node of the given path asynchronously Using
     * {@link ZookeeperAsyncStatCallbackExample} to process result
     * @param path The path of znode
     * @param acl The ACL list set to the znode
     */
    public void setACLAsync(String path, List<ACL> acl)
    {
        zookeeper.setACL(path, acl, getVersion(path), statCallback, handler);
    }

    /**
     * Set the ACL for the node of the given path synchronously
     * @param path The path of znode
     * @param data The data set to the znode
     * @return stat of the znode if correct
     */
    public Stat setDataSync(String path, byte[] data)
    {
        try
        {
            return zookeeper.setData(path, data, getVersion(path));
        }
        catch (KeeperException e)
        {
            LOG.info("Error occured when set data!");
        }
        catch (InterruptedException e)
        {
            LOG.info("Thread is interrupted when set data!");
        }
        return null;
    }

    /**
     * Set the ACL for the node of the given path asynchronously Using
     * {@link ZookeeperAsyncStatCallbackExample} to process result
     * @param path The path of znode
     * @param data The data set to the znode
     */
    public void setDataAsync(String path, byte[] data)
    {
        zookeeper.setData(path, data, getVersion(path), statCallback, handler);
    }

    /**
     * Asynchronously synchronize cluster status
     * @param path The path of znode
     */
    public void sync(String path)
    {
        zookeeper.sync(path, voidCallback, null);
    }

    /**
     * String representation of this ZooKeeper client
     */
    public String toString()
    {
        return zookeeper.toString();
    }
}
