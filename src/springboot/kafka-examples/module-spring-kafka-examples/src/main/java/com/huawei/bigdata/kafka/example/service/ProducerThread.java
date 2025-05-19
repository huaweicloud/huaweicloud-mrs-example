package com.huawei.bigdata.kafka.example.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class ProducerThread extends Thread {
  private static final Logger LOG = LoggerFactory.getLogger(ProducerThread.class);

  @Autowired
  private ProducerService producerService;

  @Value("${topic:example-metric1}")
  private String topic;

  // 默认发送100条消息
  private static final int MESSAGE_NUM = 100;

  // 指定发送多少条消息后sleep1秒
  private static final int INTEGERVAL_MESSAGES = 10;

  public ProducerThread(@Autowired ProducerService producerService) {
    this.producerService = producerService;
  }

  /**
   * 生产者线程执行函数，循环发送消息。
   */
  @Override
  public void run() {
    LOG.info("New Producer: start. The topic is " + topic);
    // 发送的消息内容
    int messageNo = 1;

    // 循环发送消息，直到达到 MESSAGE_NUM 条消息
    while (messageNo <= MESSAGE_NUM) {
      String messageStr = "Message_" + messageNo;

      // 发送消息
      producerService.sendMessage(topic, messageStr);

      messageNo++;

      // 每发送intervalMessage条消息sleep1秒
      if (messageNo % INTEGERVAL_MESSAGES == 0) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        LOG.info("The Producer have send {} messages to topic {}.", messageNo, topic);
      }
    }
    LOG.info("Finished send {} messages to topic {}", MESSAGE_NUM, topic);
  }
}
