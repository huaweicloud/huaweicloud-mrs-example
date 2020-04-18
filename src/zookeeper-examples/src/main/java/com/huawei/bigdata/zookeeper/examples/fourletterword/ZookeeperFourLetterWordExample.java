package com.huawei.bigdata.zookeeper.examples.fourletterword;

import com.huawei.bigdata.zookeeper.examples.parseconfig.ZKExampleConfigParser;
import org.apache.log4j.Logger;
import org.apache.zookeeper.client.FourLetterWordMain;
import org.apache.zookeeper.common.X509Exception.SSLContextException;

import java.io.IOException;

public class ZookeeperFourLetterWordExample
{
    private static final Logger LOG = Logger.getLogger(ZookeeperFourLetterWordExample.class.getName());

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
        String cxnString = ZKExampleConfigParser.parseCxnString();
        if (cxnString == null)
        {
            LOG.info("Error occured when parse connection String");
        }
        String address = cxnString.split(",")[0];
        String ip = address.split(":")[0];
        int port = Integer.parseInt(address.split(":")[1]);
        ZookeeperFourLetterWordExample fourletterwordexample = new ZookeeperFourLetterWordExample();
        fourletterwordexample.getConfiguration(ip, port);
        fourletterwordexample.getConnections(ip, port);
        fourletterwordexample.resetConnections(ip, port);
        fourletterwordexample.getUntreated(ip, port);
        fourletterwordexample.getEnvironment(ip, port);
        fourletterwordexample.hasNoError(ip, port);
        fourletterwordexample.resetStatistics(ip, port);
        fourletterwordexample.getDetailsForServer(ip, port);
        fourletterwordexample.getBriefForServer(ip, port);
        fourletterwordexample.getBriefForWatches(ip, port);
        fourletterwordexample.getDetailsForWatchesBySession(ip, port);
        fourletterwordexample.getDetailsForWatchesByPath(ip, port);
        fourletterwordexample.getVariables(ip, port);
    }

    /**
     * Print details about serving configuration.
     * @param host the destination host
     * @param port the destination port
     */
    public void getConfiguration(String host, int port)
    {
        try
        {
            LOG.info("Get configuration: "
                    + FourLetterWordMain.send4LetterWord(host, port, "conf"));
        }
        catch (SSLContextException | IOException e)
        {
            LOG.info("Error occured when get configuration");
        }
    }

    /**
     * List full connection/session details for all clients connected to this
     * server. Includes information on numbers of packets received/sent, session
     * id, operation latencies, last operation performed, etc...
     * @param host the destination host
     * @param port the destination port
     */
    public void getConnections(String host, int port)
    {
        try
        {
            LOG.info("Get connections: "
                    + FourLetterWordMain.send4LetterWord(host, port, "cons"));
        }
        catch (SSLContextException | IOException e)
        {
            LOG.info("Error occured when get connections");
        }
    }

    /**
     * Reset connection/session statistics for all connections
     * @param host the destination host
     * @param port the destination port
     */
    public void resetConnections(String host, int port)
    {
        try
        {
            LOG.info("Reset connections: "
                    + FourLetterWordMain.send4LetterWord(host, port, "crst"));
        }
        catch (SSLContextException | IOException e)
        {
            LOG.info("Error occured when reset connections");
        }
    }

    /**
     * Lists the outstanding sessions and ephemeral nodes. This only works on
     * the leader.
     * @param host the destination host
     * @param port the destination port
     */
    public void getUntreated(String host, int port)
    {
        try
        {
            LOG.info("Get untreated session&node: "
                    + FourLetterWordMain.send4LetterWord(host, port, "dump"));
        }
        catch (SSLContextException | IOException e)
        {
            LOG.info("Error occured when get untreated session&node");
        }
    }

    /**
     * Print details about serving environment
     * @param host the destination host
     * @param port the destination port
     */
    public void getEnvironment(String host, int port)
    {
        try
        {
            LOG.info("Get serving environment: "
                    + FourLetterWordMain.send4LetterWord(host, port, "envi"));
        }
        catch (SSLContextException | IOException e)
        {
            LOG.info("Error occured when get serving environment");
        }
    }

    /**
     * Tests if server is running in a non-error state. The server will respond
     * with imok if it is running. Otherwise it will not respond at all. A
     * response of "imok" does not necessarily indicate that the server has
     * joined the quorum, just that the server process is active and bound to
     * the specified client port. Use "stat" for details on state wrt quorum and
     * client connection information.
     * @param host the destination host
     * @param port the destination port
     */
    public void hasNoError(String host, int port)
    {
        try
        {
            LOG.info("Test server state: "
                    + FourLetterWordMain.send4LetterWord(host, port, "ruok"));
        }
        catch (SSLContextException | IOException e)
        {
            LOG.info("Error occured when test server state");
        }
    }

    /**
     * Reset server statistics.
     * @param host the destination host
     * @param port the destination port
     */
    public void resetStatistics(String host, int port)
    {
        try
        {
            LOG.info("Reset server statistics: "
                    + FourLetterWordMain.send4LetterWord(host, port, "srst"));
        }
        catch (SSLContextException | IOException e)
        {
            LOG.info("Error occured when reset server statistics");
        }
    }

    /**
     * Lists full details for the server.
     * @param host the destination host
     * @param port the destination port
     */
    public void getDetailsForServer(String host, int port)
    {
        try
        {
            LOG.info("Lists full details for the server: "
                    + FourLetterWordMain.send4LetterWord(host, port, "srvr"));
        }
        catch (SSLContextException | IOException e)
        {
            System.out
                    .println("Error occured when lists full details for the server");
        }
    }

    /**
     * Lists brief details for the server and connected clients.
     * @param host the destination host
     * @param port the destination port
     */
    public void getBriefForServer(String host, int port)
    {
        try
        {
            LOG.info("Lists brief details for the server: "
                    + FourLetterWordMain.send4LetterWord(host, port, "stat"));
        }
        catch (SSLContextException | IOException e)
        {
            System.out
                    .println("Error occured when lists brief details for the server");
        }
    }

    /**
     * Lists brief information on watches for the server.
     * @param host the destination host
     * @param port the destination port
     */
    public void getBriefForWatches(String host, int port)
    {
        try
        {
            LOG.info("Lists brief details for the watcher: "
                    + FourLetterWordMain.send4LetterWord(host, port, "wchs"));
        }
        catch (SSLContextException | IOException e)
        {
            System.out
                    .println("Error occured when lists brief details for the watcher");
        }
    }

    /**
     * Lists detailed information on watches for the server, by session. This
     * outputs a list of sessions(connections) with associated watches (paths).
     * Note, depending on the number of watches this operation may be expensive
     * (ie impact server performance), use it carefully.
     * @param host the destination host
     * @param port the destination port
     */
    public void getDetailsForWatchesBySession(String host, int port)
    {
        try
        {
            System.out
                    .println("Lists full details for the watcher by session: "
                            + FourLetterWordMain.send4LetterWord(host, port,
                                    "wchc"));
        }
        catch (SSLContextException | IOException e)
        {
            System.out
                    .println("Error occured when lists full details for the watcher by session");
        }
    }

    /**
     * Lists detailed information on watches for the server, by path. This
     * outputs a list of paths (znodes) with associated sessions. Note,
     * depending on the number of watches this operation may be expensive (ie
     * impact server performance), use it carefully. (ie impact server
     * performance), use it carefully.
     * @param host the destination host
     * @param port the destination port
     */
    public void getDetailsForWatchesByPath(String host, int port)
    {
        try
        {
            LOG.info("Lists full details for the watcher by path: "
                    + FourLetterWordMain.send4LetterWord(host, port, "wchp"));
        }
        catch (SSLContextException | IOException e)
        {
            System.out
                    .println("Error occured when lists full details for the watcher by path");
        }
    }

    /**
     * Outputs a list of variables that could be used for monitoring the health
     * of the cluster.
     * @param host the destination host
     * @param port the destination port
     */
    public void getVariables(String host, int port)
    {
        try
        {
            LOG.info("Lists variables: "
                    + FourLetterWordMain.send4LetterWord(host, port, "mntr"));
        }
        catch (SSLContextException | IOException e)
        {
            LOG.info("Error occured when lists variables");
        }
    }
}
