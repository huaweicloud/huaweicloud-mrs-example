package com.huawei.bigdata.zookeeper.examples.client;

import com.huawei.bigdata.zookeeper.examples.parseconfig.ZKExampleConfigParser;
import com.huawei.bigdata.zookeeper.examples.watcher.ZookeeperConnectWatcherExample;
import com.huawei.hadoop.security.LoginUtil;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * This class is designed to explain how to use watcher This class simulate a
 * scenes: There are three client: CreaterClient, WriterClient and ReaderClient,
 * ReaderClient and WriterWatcher will register watchers on path "/znodeRW" Then
 * CreaterClient will create a znoed on path "/znodeRW", watchers watch this
 * operation and send enents to ReaderClient and WriterClient. When receive a
 * event of {@link Watcher.Event.EventType.NodeCreated}, ReaderClient will read
 * the data written by CreaterClient and then WriteClient will change the data
 * of "/znode". When ReaderClient receive a {@link Watcher.Event.EventType}
 * .NodeDataChanged event, ReadClient will read the data written by WriteClient
 * and delete the node.
 */
public class ZookeeperMutualClientExample
{
    private static final Logger LOG = Logger.getLogger(ZookeeperMutualClientExample.class.getName());

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

    public static void main(String[] args)
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
            LOG.info("Error occured when parse connection String");
        }
        int sessionTimeout = Integer.parseInt(ZKExampleConfigParser
                .parseTimeout());
        ZookeeperMutualClientExample mutualclientexample = new ZookeeperMutualClientExample(
                cxnString, sessionTimeout);
        mutualclientexample.runExample();
    }

    String cxnString;

    int sessionTimeout;

    public ZookeeperMutualClientExample(String cxnString, int sessionTimeout)
    {
        this.cxnString = cxnString;
        this.sessionTimeout = sessionTimeout;
    }

    /**
     * These two object used to help coordinate thread
     */
    private Object read = new Object();

    private CountDownLatch readwritelatch = new CountDownLatch(2);

    /**
     * CreaterClient will create a znode on path "/znodeRW"
     */
    public class CreaterClient implements Runnable
    {

        /**
         * After ReaderClient and WriterClient register watchers, create a znode
         */
        @Override
        public void run()
        {
            try
            {
                readwritelatch.await();
                CountDownLatch countDownLatch = new CountDownLatch(1);
                ZookeeperConnectWatcherExample watcher = new ZookeeperConnectWatcherExample(
                        countDownLatch);
                ZooKeeper zookeeper = new ZooKeeper(cxnString, sessionTimeout,
                        watcher);
                countDownLatch.await();
                LOG.info("CreaterClient established");
                LOG.info("CreaterClient create: "
                        + zookeeper.create("/znodeRW", "create".getBytes(),
                                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
            }
            catch (KeeperException | InterruptedException | IOException e)
            {
                LOG.info("Error occured when init zookeeper createclient or create znode");
            }
        }
    }

    /**
     * WriterClient will change the data on path "/znodeRW"
     */
    public class WriterClient implements Runnable
    {
        ZooKeeper zookeeper;

        /**
         * Init a zookeeper client
         */
        public WriterClient()
        {
            try
            {
                CountDownLatch countDownLatch = new CountDownLatch(1);
                ZookeeperConnectWatcherExample watcher = new ZookeeperConnectWatcherExample(
                        countDownLatch);
                zookeeper = new ZooKeeper(cxnString, sessionTimeout, watcher);
                countDownLatch.await();
            }
            catch (IOException | InterruptedException e)
            {
                LOG.info("Error occured when init zookeeper writeclient");
            }
        }

        /**
         * A simple watcher used to watcher the NodeCreated event
         */
        class WriterWatcher implements Watcher
        {

            /**
             * process received WatchedEvent
             * 
             * @param event The received WatchedEvent to process
             */
            @Override
            public void process(WatchedEvent event)
            {
                try
                {
                    zookeeper.exists("/znodeRW", this);
                }
                catch (KeeperException | InterruptedException e)
                {
                    LOG.info("Error occured when register watcher");
                }
                if (event.getType() == Event.EventType.NodeCreated)
                {
                    LOG.info("WriterWatcher receive NodeCreated event");
                    synchronized (WriterClient.this)
                    {
                        WriterClient.this.notify();
                    }
                }

            }

        }

        /**
         * After CreaterClient create znode and ReaderClient read data, change
         * the data
         */
        @Override
        public void run()
        {
            try
            {
                LOG.info("WriteClient established");
                zookeeper.exists("/znodeRW", new WriterWatcher());
                readwritelatch.countDown();
                synchronized (this)
                {
                    LOG.info("WriteClient wait for create");
                    this.wait();
                }
                synchronized (read)
                {
                    LOG.info("WriteClient wait for read");
                    read.wait();
                }
                zookeeper.setData("/znodeRW", "changedata".getBytes(),
                        zookeeper.exists("/znodeRW", false).getVersion());
                LOG.info("WriteClient change data: changedata");
                LOG.info("list all znodes: " + zookeeper.getChildren("/", false));
            }
            catch (InterruptedException | KeeperException e)
            {
                LOG.info("Error occured when change znode");
            }
        }
    }

    /**
     * ReaderClient read the data on path "/znodeRW"
     */
    public class ReaderClient implements Runnable
    {
        ZooKeeper zookeeper;

        Object create = new Object();

        Object change = new Object();

        /**
         * Init a zookeeper Client
         */
        public ReaderClient()
        {
            try
            {
                CountDownLatch countDownLatch = new CountDownLatch(1);
                ZookeeperConnectWatcherExample watcher = new ZookeeperConnectWatcherExample(
                        countDownLatch);
                zookeeper = new ZooKeeper(cxnString, sessionTimeout, watcher);
                countDownLatch.await();
            }
            catch (IOException | InterruptedException e)
            {
                LOG.info("Error occured when init zookeeper readclient");
            }
        }

        /**
         * A simple watcher used to watch NodeDataChanged event and NodeCreated
         * event
         */
        class ReaderWatcher implements Watcher
        {
            /**
             * process received WatchedEvent
             * 
             * @param event The received WatchedEvent to process
             */
            @Override
            public void process(WatchedEvent event)
            {
                try
                {
                    zookeeper.exists("/znodeRW", this);
                }
                catch (KeeperException | InterruptedException e)
                {
                    LOG.info("Error occured when register watcher");
                }

                if (event.getType() == Event.EventType.NodeDataChanged)
                {
                    LOG.info("ReaderWatcher receive NodeDataChanged event");
                    synchronized (change)
                    {
                        change.notify();
                    }
                }
                if (event.getType() == Event.EventType.NodeCreated)
                {
                    LOG.info("ReaderWatcher receive NodeCreated event");
                    synchronized (create)
                    {
                        create.notify();
                    }
                }

            }

        }

        /**
         * Read the data after create or change znode
         */
        @Override
        public void run()
        {
            try
            {
                LOG.info("ReadClient established");
                zookeeper.exists("/znodeRW", new ReaderWatcher());
                readwritelatch.countDown();
                synchronized (create)
                {
                    LOG.info("ReadClient wait for create");
                    create.wait();
                }

                LOG.info("ReadClient read /znodeRW: "
                        + new String(zookeeper.getData("/znodeRW", false, null)));
                synchronized (read)
                {
                    read.notify();
                }
                synchronized (change)
                {
                    LOG.info("ReadClient wait for change");
                    change.wait();
                }
                LOG.info("ReadClient read /znodeRW: "
                        + new String(zookeeper.getData("/znodeRW", false, null)));
            }
            catch (KeeperException | InterruptedException e)
            {
                LOG.info("Error occured when read znode");
            }
            finally
            {
                try
                {
                    zookeeper.delete("/znodeRW",
                            zookeeper.exists("/znodeRW", false).getVersion());
                }
                catch (InterruptedException | KeeperException e)
                {
                    LOG.info("Error occured when delete znode");
                }
            }
        }
    }

    public void runExample()
    {
        Thread readerClient = new Thread(new ReaderClient());
        Thread writerClient = new Thread(new WriterClient());
        Thread createrClient = new Thread(new CreaterClient());
        readerClient.start();
        writerClient.start();
        createrClient.start();
    }
}
