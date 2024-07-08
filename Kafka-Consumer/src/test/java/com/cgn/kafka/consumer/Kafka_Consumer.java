package com.cgn.kafka.consumer;

import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Kafka_Consumer {
  private static final String KAFKA_SERVER = "127.0.0.1:9092";
  private static final String GROUP_ID = "ZGH";
  private static final String TOPIC = "root.zgh";
  private static final String LOCAL_HOST = "localhost";
  private static final String USERNAME = "root";
  private static final String PASSWORD = "root";
  private static KafkaConsumer kafkaConsumer;
  private static Session session;
  private static ExecutorService service = Executors.newFixedThreadPool(10);

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    kafkaConsumer = new KafkaConsumer<>(props);
    kafkaConsumer.subscribe(Arrays.asList(TOPIC));
    session =
        new Session.Builder()
            .host(LOCAL_HOST)
            .port(6667)
            .username(USERNAME)
            .password(PASSWORD)
            .version(Version.V_1_0)
            .build();
    session.open(false);
    session.setFetchSize(10000);

    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  kafkaConsumer.close();
                  service.shutdown();
                  try {
                    session.close();
                  } catch (IoTDBConnectionException e) {
                    throw new RuntimeException(e);
                  }
                }));
    Thread consumerThread =
        new Thread(
            () -> {
              while (true) {
                ConsumerRecords<String, String> consumerRecords =
                    kafkaConsumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord consumerRecord : consumerRecords) {
                  service.submit(
                      () -> {
                        try {
                          insertBySession(consumerRecord);
                        } catch (Exception e) {
                          e.printStackTrace();
                        }
                      });
                }
              }
            });
    consumerThread.start();
  }

  public static void insertBySession(ConsumerRecord<String, String> consumerRecord)
      throws IoTDBConnectionException, StatementExecutionException {
    JSONObject jsonObject = JSONObject.parseObject(String.valueOf(consumerRecord.value()));
    String device = jsonObject.getString("device");
    Long timestamp = jsonObject.getLong("timestamp");
    String sensorStr = jsonObject.getString("sensors").replaceAll("\\s", "");
    List<String> sensors = Arrays.asList(sensorStr.substring(1, sensorStr.length() - 1).split(","));
    String typeStr = jsonObject.getString("types");
    String[] split = typeStr.substring(1, typeStr.length() - 1).split(",");
    List<TSDataType> list = new ArrayList<>();
    for (String type : split) {
      list.add(TSDataType.valueOf(type.trim()));
    }
    String valuesStr = jsonObject.getString("values").replaceAll("\\s", "");
    String[] split1 = valuesStr.substring(1, valuesStr.length() - 1).trim().split(",");
    List<Object> values = new ArrayList<>();
    for (int i = 0; i < split1.length; i++) {
      values.add(parseValue(split1[i], list.get(i)));
    }
    session.insertRecord(device, timestamp, sensors, list, values);
  }

  private static Object parseValue(String valueStr, TSDataType type) {
    String trim = valueStr.trim();
    switch (type) {
      case BOOLEAN:
        return Boolean.valueOf(trim);
      case INT32:
        return Integer.valueOf(trim);
      case INT64:
        return Long.valueOf(trim);
      case FLOAT:
        return Float.valueOf(trim);
      case DOUBLE:
        return Double.valueOf(trim);
      case TEXT:
        return trim;
      default:
        throw new IllegalArgumentException("Unsupported TSDataType: " + type);
    }
  }
}
