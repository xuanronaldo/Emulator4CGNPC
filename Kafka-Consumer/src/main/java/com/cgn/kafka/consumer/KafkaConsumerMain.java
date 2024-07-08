package com.cgn.kafka.consumer;

import org.apache.iotdb.rpc.IoTDBConnectionException;

import java.util.concurrent.atomic.AtomicInteger;

public class KafkaConsumerMain {
  public static void main(String[] args) throws IoTDBConnectionException {
    Config config = new Config("config.properties");
    AtomicInteger count = new AtomicInteger(0);
    for (int i = 0; i < Integer.parseInt(config.getProperty("partition")); i++) {
      KafkaConsumerThread consumerThread = new KafkaConsumerThread(config, count);
      Thread thread = new Thread(consumerThread);
      thread.start();
    }
  }
}
