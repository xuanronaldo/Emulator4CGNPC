package com.cgn.kafka.producer;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import com.cgn.iot.benchmark.conf.Config;
import com.cgn.iot.benchmark.conf.ConfigDescriptor;
import com.cgn.iot.benchmark.entity.Batch.IBatch;
import com.cgn.iot.benchmark.entity.Record;
import com.cgn.iot.benchmark.entity.Sensor;
import com.cgn.iot.benchmark.exception.DBConnectException;
import com.cgn.iot.benchmark.measurement.Status;
import com.cgn.iot.benchmark.schema.schemaImpl.DeviceSchema;
import com.cgn.iot.benchmark.tsdb.DBConfig;
import com.cgn.iot.benchmark.tsdb.IDatabase;
import com.cgn.iot.benchmark.tsdb.TsdbException;
import com.cgn.iot.benchmark.workload.query.impl.*;
import com.google.gson.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class BenchmarkToKafka implements IDatabase {
  private static Producer<String, String> producer = null;
  protected static DBConfig dbConfig = null;
  protected static final Config config = ConfigDescriptor.getInstance().getConfig();
  private static final Logger LOGGER = LoggerFactory.getLogger(BenchmarkToKafka.class);
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
  protected final String ROOT_SERIES_NAME = config.getTOPIC_NAME();

  public BenchmarkToKafka(DBConfig dbConfig) throws TsdbException {
    this.dbConfig = dbConfig;
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKAFKA_LOCATION());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producer = new KafkaProducer<>(props);
  }

  @Override
  public void init() throws TsdbException {}

  @Override
  public void cleanup() throws TsdbException {}

  @Override
  public void close() throws TsdbException {
    try {
      if (producer != null) {
        producer.flush();
        producer.close();
      }
      LOGGER.info("Kafka producer close!");
    } catch (Exception e) {
      LOGGER.error("Kafka closed failed!");
    }
  }

  @Override
  public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    return null;
  }

  @Override
  public Status insertOneBatch(IBatch batch) throws DBConnectException {
    return insertOneBatchByRecord(batch);
  }

  /**
   * 单条数据异步发送
   *
   * @param batch
   * @return
   * @throws DBConnectException
   */
  public Status insertOneBatchByRecord(IBatch batch) {
    String deviceId = getDevicePath(batch.getDeviceSchema());
    List<String> sensors =
        batch.getDeviceSchema().getSensors().stream()
            .map(Sensor::getName)
            .collect(Collectors.toList());
    for (Record record : batch.getRecords()) {
      long timestamp = record.getTimestamp();
      List<TSDataType> dataTypes =
          constructDataTypes(
              batch.getDeviceSchema().getSensors(), record.getRecordDataValue().size());
      KafkaRecord kafkaRecord =
          new KafkaRecord(deviceId, sensors, timestamp, dataTypes, record.getRecordDataValue());
      //      String json = gson.toJson(kafkaRecord);
      String recordString = kafkaRecord.toString();
      int partition = getPartition(deviceId);
      producer.send(
          new ProducerRecord<>(config.getTOPIC_NAME(), partition, null, recordString),
          new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
              if (e != null) {
                e.printStackTrace();
              }
            }
          });
    }
    return new Status(true);
  }

  /**
   * batch数据发送 可能由于数据过大，会出现发送失败的问题
   *
   * @param batch
   * @return
   */
  public Status insertOneBatchByRecords(IBatch batch) {
    String deviceId = getDevicePath(batch.getDeviceSchema());
    List<KafkaRecord> kafkaRecords = new ArrayList<>();
    List<String> sensors =
        batch.getDeviceSchema().getSensors().stream()
            .map(Sensor::getName)
            .collect(Collectors.toList());
    //    long startTime = System.nanoTime();
    for (Record record : batch.getRecords()) {
      long timestamp = record.getTimestamp();
      List<TSDataType> dataTypes =
          constructDataTypes(
              batch.getDeviceSchema().getSensors(), record.getRecordDataValue().size());
      //      需要优化一个多设备数据批次插入Kafka吗
      KafkaRecord kafkaRecord =
          new KafkaRecord(deviceId, sensors, timestamp, dataTypes, record.getRecordDataValue());
      kafkaRecords.add(kafkaRecord);
      //      String recordString = kafkaRecord.toString();
      //      int partition = getPartition(deviceId);
      //      producer.send(new ProducerRecord<>(config.getTOPIC_NAME(), partition, null,
      // recordString));
      //      producer.send(
      //          new ProducerRecord<>(config.getTOPIC_NAME(), recordString, recordString),
      //          new Callback() {
      //            @Override
      //            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      //              if (e != null) {
      ////                e.printStackTrace();
      //              }
      //            }
      //          });
    }
    KafkaRecords records = new KafkaRecords(kafkaRecords);
    String json = gson.toJson(records);
    try {
      producer
          .send(new ProducerRecord<>(config.getTOPIC_NAME(), getPartition(json), null, json))
          .get();
    } catch (Exception e) {
      e.printStackTrace();
    }
    producer.flush();
    return new Status(true);
  }

  private int getPartition(String key) {
    return Math.abs(key.hashCode()) % config.getKAFKA_PARTITION();
  }

  protected String getDevicePath(DeviceSchema deviceSchema) {
    StringBuilder name = new StringBuilder("root.cgn");
    name.append(".").append(deviceSchema.getGroup());
    for (Map.Entry<String, String> pair : deviceSchema.getTags().entrySet()) {
      name.append(".").append(pair.getValue());
    }
    name.append(".").append(deviceSchema.getDevice());
    return name.toString();
  }

  public List<TSDataType> constructDataTypes(List<Sensor> sensors, int recordValueSize) {
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int sensorIndex = 0; sensorIndex < recordValueSize; sensorIndex++) {
      switch (sensors.get(sensorIndex).getSensorType()) {
        case BOOLEAN:
          dataTypes.add(TSDataType.BOOLEAN);
          break;
        case INT32:
          dataTypes.add(TSDataType.INT32);
          break;
        case INT64:
          dataTypes.add(TSDataType.INT64);
          break;
        case FLOAT:
          dataTypes.add(TSDataType.FLOAT);
          break;
        case DOUBLE:
          dataTypes.add(TSDataType.DOUBLE);
          break;
        case TEXT:
          dataTypes.add(TSDataType.TEXT);
          break;
      }
    }
    return dataTypes;
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    return null;
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    return null;
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    return null;
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    return null;
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    return null;
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    return null;
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    return null;
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    return null;
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    return null;
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    return null;
  }
}
