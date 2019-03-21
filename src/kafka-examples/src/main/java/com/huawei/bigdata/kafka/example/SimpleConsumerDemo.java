package com.huawei.bigdata.kafka.example;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.BrokerEndPoint;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class SimpleConsumerDemo
{
    private static Logger log = Logger.getLogger(SimpleConsumerDemo.class);
    
    // hostName和port
    private Map<String, Integer> replicaBrokers = new HashMap<String, Integer>();
    
    private static final int BUFFERSIZE = 64 * 1024;
    
    private static final int SOTIMEOUT = 100000;
    
    private static final int FETCHSIZE = 100000;
    
    private static final String DEFAULTPORT = "9092";
    
    public SimpleConsumerDemo()
    {
    }
    
    public void run(long maxReads, String topic, int partition, Map<String, Integer> ipPort)
        throws Exception
    {
        // find the meta data about the topic and partition we are interested in
        PartitionMetadata metadata = findLeader(ipPort, topic, partition);
        if (metadata == null)
        {
            log.error("Can't find metadata for Topic and Partition. Exiting");
            return;
        }
        
        if (metadata.leader() == null)
        {
            log.error("Can't find Leader for Topic and Partition. Exiting");
            return;
        }
        
        // hostName
        String leadBroker = metadata.leader().host();
       
        String clientName = "Client_" + topic + "_" + partition;
        
        log.info("SimpleExample: find leadBroker is " + leadBroker);
        log.info("SimpleExample: clientName is " + clientName);
        
        int port = 0;
        try 
        {
        	port = Integer.valueOf(KafkaProperties.getInstance().getValues("port", DEFAULTPORT));
        }
        catch(Exception e)
        {
        	log.error("get port error.");
        }
        
        SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, SOTIMEOUT, BUFFERSIZE, clientName);
        long readOffset =
            getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
        log.info("SimpleExample: getLastOffset is " + readOffset);
        
        int numErrors = 0;
        while (maxReads > 0)
        {
            if (consumer == null)
            {
                consumer = new SimpleConsumer(leadBroker, port, SOTIMEOUT, BUFFERSIZE, clientName);
            }
            FetchRequest req =
                new FetchRequestBuilder().clientId(clientName).addFetch(topic, partition, readOffset, FETCHSIZE) // Note: this FETCHSIZE of 100000 might need to be increased if large batches are written to Kafka
                    .build();
            FetchResponse fetchResponse = consumer.fetch(req);
            
            if (fetchResponse.hasError())
            {
                numErrors++;
                // Something went wrong!
                short code = fetchResponse.errorCode(topic, partition);
                log.info("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
                
                // 5次错误后不再尝试
                if (numErrors > 5)
                {
                    break;
                }

                if (code == ErrorMapping.OffsetOutOfRangeCode())
                {
                    // We asked for an invalid offset. For simple case ask for the last element to reset
                    readOffset =
                        getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
                    continue;
                }
                consumer.close();
                consumer = null;
                leadBroker = findNewLeader(leadBroker, topic, partition, port);
                continue;
            }
            numErrors = 0;
            
            long numRead = 0;
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition))
            {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < readOffset)
                {
                    log.info("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                    continue;
                }
                readOffset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();
                
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                log.info(String.valueOf(messageAndOffset.offset()) + ": " + new String(bytes, "UTF-8"));
                numRead++;
                maxReads--;
            }
            
            if (numRead == 0)
            {
                try
                {
                    Thread.sleep(1000);
                }
                catch (InterruptedException ie)
                {
                }
            }
        }
        
        if (consumer != null)
        {
            consumer.close();
        }
    }
    
    /**
     * 获取topic的偏移
     * @param consumer   消费端
     * @param topic      主题
     * @param partition  分区
     * @param whichTime  时间  kafka.api.OffsetRequest.EarliestTime()-开始；kafka.api.OffsetRequest.LatestTime()-当前。
     * @param clientName 客户端名称
     * @return
     */
    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime,
        String clientName)
    {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo =
            new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        
        kafka.javaapi.OffsetRequest request =
            new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        
        OffsetResponse response = consumer.getOffsetsBefore(request);
        
        if (response.hasError())
        {
            log.info("Error fetching data Offset Data the Broker. Reason: "
                + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }
    
    /**
     * 查找新的leader
     * @param oldLeader   原来的leader
     * @param topic       主题
     * @param partition   分区
     * @param port        端口号
     * @return
     */
    private String findNewLeader(String oldLeader, String topic, int partition, int port) throws Exception
    {
        // 尝试3次
        for (int i = 0; i < 3; i++)
        {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(replicaBrokers, topic, partition);
            if (metadata == null)
            {
                goToSleep = true;
            }
            else if (metadata.leader() == null)
            {
                goToSleep = true;
            }
            else if (oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0)
            {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            }
            else
            {
                return metadata.leader().host();
            }
            
            if (goToSleep)
            {
                try
                {
                    Thread.sleep(1000);
                }
                catch (InterruptedException ie)
                {
                    log.info("Thread occur a InterruptedException.");
                }
            }
        }
        log.info("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }
    
    /**
     * 查找leader节点
     * @param ipPorts      ip端口号
     * @param topic       主题
     * @param partition   分区
     * @return
     */
    private PartitionMetadata findLeader(Map<String, Integer> ipPorts, String topic, int partition)
    {
        PartitionMetadata returnMetaData = null;
        loop: for (String hostIp : ipPorts.keySet())
        {
            SimpleConsumer consumer = null;
            try
            {
                consumer = new SimpleConsumer(hostIp, ipPorts.get(hostIp), SOTIMEOUT, BUFFERSIZE, "leaderLookup");
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
                
                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData)
                {
                    for (PartitionMetadata part : item.partitionsMetadata())
                    {
                        if (part.partitionId() == partition)
                        {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                log.error("Communicating with Broker [" + hostIp + "] to find Leader for [" + topic
                    + ", " + partition + "] Reason: " + e);
            }
            finally
            {
                if (consumer != null)
                    consumer.close();
            }
        }
        if (returnMetaData != null)
        {
            replicaBrokers.clear();
            for (BrokerEndPoint replica : returnMetaData.replicas())
            {
                replicaBrokers.put(replica.host(), replica.port());
            }
        }
        return returnMetaData;
    }
    
    /**
     * 根据String brokerList获取Broker的Ip和端口号的Map结果
     * @param brokerList Broker列表
     * @return
     */
    public static Map<String, Integer> getIpPortMap(String brokerList)
    {
        Map<String, Integer> ipPort = new HashMap<String, Integer>();
        
        String[] brokerArray = brokerList.split(",");
        
        for (String oneBk : brokerArray)
        {
            String[] arrayIpPort = oneBk.split(":");
            
            // 端口号
            int port = 0;
            
            try
            {
                port = Integer.valueOf(arrayIpPort[1]);
            }
            catch (Exception e)
            {
                log.error("Port is error.");
            }
            
            ipPort.put(arrayIpPort[0], port);
        }
        
        return ipPort;
    }
    
    public static void main(String args[])
    {
        // 允许读取的最大消息数
        long maxReads = 0;
        
        try
        {
            maxReads = Long.valueOf(args[0]);
        }
        catch (Exception e)
        {
            log.error("args[0] should be a number for maxReads.\n" + 
                "args[1] should be a string for topic. \n" + 
                "args[2] should be a number for partition.");
            return;
        }
        
        if (null == args[1])
        {
            log.error("args[0] should be a number for maxReads.\n" + 
                "args[1] should be a string for topic. \n" + 
                "args[2] should be a number for partition.");
            return;
        }
        
        // 消费的消息主题
        // String topic = KafkaProperties.TOPIC;
        String topic = args[1];
        
        // 消息的消息分区
        int partition = 0;
        try 
        {
            partition = Integer.parseInt(args[2]);
        }
        catch (Exception e)
        {
            log.error("args[0] should be a number for maxReads.\n" + 
                "args[1] should be a string for topic. \n" + 
                "args[2] should be a number for partition.");
        }
        
        // Broker List
        String bkList = KafkaProperties.getInstance().getValues("metadata.broker.list", "localhost:9092");
        
        Map<String, Integer> ipPort = getIpPortMap(bkList);
        
        SimpleConsumerDemo example = new SimpleConsumerDemo();
        try
        {
            example.run(maxReads, topic, partition, ipPort);
        }
        catch (Exception e)
        {
            log.info("Oops:" + e);
            e.printStackTrace();
        }
    }
}
