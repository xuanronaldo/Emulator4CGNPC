package com.cgn.kafka.consumer;

import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaConsumerThread implements Runnable {

  private final KafkaConsumer<String, String> consumer;
  private final Session session;
  long start = System.currentTimeMillis();
  private AtomicInteger count;
  private static Gson gson =
      new GsonBuilder()
          .registerTypeAdapter(
              Number.class,
              new JsonDeserializer<Number>() {
                @Override
                public Number deserialize(
                    JsonElement json, Type typeOfT, JsonDeserializationContext context)
                    throws JsonParseException {
                  // 如果是整数形式，返回Integer，否则返回Double
                  if (json.getAsJsonPrimitive().isNumber()) {
                    double value = json.getAsDouble();
                    if (value == (int) value) {
                      return (int) value;
                    } else {
                      return value;
                    }
                  } else {
                    throw new JsonParseException("Expected a number");
                  }
                }
              })
          .registerTypeAdapter(
              Number.class,
              new JsonSerializer<Number>() {
                @Override
                public JsonElement serialize(
                    Number src, Type typeOfSrc, JsonSerializationContext context) {
                  // 根据类型进行序列化
                  if (src instanceof Double) {
                    double value = (Double) src;
                    if (value == (int) value) {
                      return new JsonPrimitive((int) value);
                    } else {
                      return new JsonPrimitive(value);
                    }
                  } else if (src instanceof Integer) {
                    return new JsonPrimitive((Integer) src);
                  } else {
                    return new JsonPrimitive(src.toString());
                  }
                }
              })
          .create();

  public KafkaConsumerThread(Config config, AtomicInteger count) throws IoTDBConnectionException {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty("broker"));
    props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getProperty("group_id"));
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    this.count = count;
    consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(config.getProperty("topic")));
    session =
        new Session.Builder()
            .host(config.getProperty("iotdb_host"))
            .port(Integer.parseInt(config.getProperty("iotdb_port")))
            .username(config.getProperty("iotdb_username"))
            .password(config.getProperty("iotdb_password"))
            .version(Version.V_1_0)
            .build();
    session.open(false);
    session.setFetchSize(Integer.parseInt(config.getProperty("iotdb_fetch_size")));
  }
  //  public KafkaConsumerThread(Config config) throws IoTDBConnectionException {
  //    Properties props = new Properties();
  //    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty("broker"));
  //    props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getProperty("group_id"));
  //    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
  //    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
  // StringDeserializer.class.getName());
  //    consumer = new KafkaConsumer<>(props);
  //    consumer.subscribe(Collections.singletonList(config.getProperty("topic")));
  //    session =
  //            new Session.Builder()
  //                    .host(config.getProperty("iotdb_host"))
  //                    .port(Integer.parseInt(config.getProperty("iotdb_port")))
  //                    .username(config.getProperty("iotdb_username"))
  //                    .password(config.getProperty("iotdb_password"))
  //                    .version(Version.V_1_0)
  //                    .build();
  //    session.open(false);
  //    session.setFetchSize(Integer.parseInt(config.getProperty("iotdb_fetch_size")));
  //  }

  @Override
  public void run() {
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(1000);
      if (records.count() == 0) {
        continue;
      }
      try {
        insertRecordsBySession(records);
      } catch (IoTDBConnectionException e) {
        throw new RuntimeException(e);
      } catch (StatementExecutionException e) {
        throw new RuntimeException(e);
      }
      //        单条插入
      //        for (ConsumerRecord record : records) {
      //        try {
      //          insertBySession(record);
      //        } catch (IoTDBConnectionException | StatementExecutionException e) {
      //          throw new RuntimeException(e);
      //        }
      //      }
    }
  }

  public void insertBySession(ConsumerRecord<String, String> consumerRecord)
      throws IoTDBConnectionException, StatementExecutionException {
    //    TODO:如何判断是通过什么方式发送的数据：record？records
    JSONObject jsonObject = JSONObject.parseObject(String.valueOf(consumerRecord.value()));
    System.out.println(consumerRecord.toString());
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
    this.count.incrementAndGet();
  }

  //  TODO：生产者根消费者的record的粒度一样，多加一层分隔就行
  public void insertRecordsBySession(ConsumerRecords<String, String> records)
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> devices = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    //      TODO：如果是一个batch是一个record，就要先对record进行一个解析
    for (ConsumerRecord record : records) {
      KafkaRecord kafkaRecord = getRecord(record);
      //      KafkaRecord kafkaRecord = gson.fromJson((String) record.value(), KafkaRecord.class);
      System.out.println(kafkaRecord.toString());
      devices.add(kafkaRecord.getDeviceId());
      times.add(kafkaRecord.getTimestamp());
      measurementsList.add(kafkaRecord.getSensors());
      typesList.add(kafkaRecord.getTypes());
      valuesList.add(kafkaRecord.getValues());
    }
    session.insertRecords(devices, times, measurementsList, typesList, valuesList);
  }

  public KafkaRecord getRecord(ConsumerRecord<String, String> consumerRecord) {
    //          报错位置定位到json转换回来，int都是double类型的，还需要进行一次转换
    JSONObject jsonObject = JSONObject.parseObject(String.valueOf(consumerRecord.value()));
    //    System.out.println(jsonObject.toString());
    String device = jsonObject.getString("device");
    Long timestamp = jsonObject.getLong("timestamp");
    String sensorStr = jsonObject.getString("sensors").replaceAll("\\s", "");
    List<String> sensors = Arrays.asList(sensorStr.substring(1, sensorStr.length() - 1).split(","));
    String typeStr = jsonObject.getString("types");
    String[] typeSplit = typeStr.substring(1, typeStr.length() - 1).split(",");
    List<TSDataType> types = new ArrayList<>();
    for (String type : typeSplit) {
      types.add(TSDataType.valueOf(type.trim()));
    }
    String valuesStr = jsonObject.getString("values").replaceAll("\\s", "");
    String[] split1 = valuesStr.substring(1, valuesStr.length() - 1).trim().split(",");
    List<Object> values = new ArrayList<>();
    for (int i = 0; i < split1.length; i++) {
      values.add(parseValue(split1[i], types.get(i)));
    }
    return new KafkaRecord(device, sensors, timestamp, types, values);
  }

  public KafkaRecord gsonCon(ConsumerRecord<String, String> consumerRecord) {
    KafkaRecord kafkaRecord = gson.fromJson(consumerRecord.value(), KafkaRecord.class);
    List<Object> values = kafkaRecord.getValues();
    List<TSDataType> types = kafkaRecord.getTypes();
    List<Object> realValues = new ArrayList<>();
    for (int i = 0; i < values.size(); i++) {
      realValues.add(parseValue(String.valueOf(values.get(i)), types.get(i)));
    }
    kafkaRecord.setValues(realValues);
    return kafkaRecord;
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
