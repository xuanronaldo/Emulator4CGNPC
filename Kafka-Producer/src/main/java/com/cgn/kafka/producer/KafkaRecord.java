package com.cgn.kafka.producer;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

public class KafkaRecord {
  String deviceId;
  List<String> sensors;
  Long timestamp;
  List<TSDataType> types;
  List<Object> values;

  public KafkaRecord(
      String deviceId,
      List<String> sensors,
      Long timestamp,
      List<TSDataType> types,
      List<Object> values) {
    this.deviceId = deviceId;
    this.timestamp = timestamp;
    this.types = types;
    this.values = values;
    this.sensors = sensors;
  }

  @Override
  public String toString() {
    return String.format(
        "{\n"
            + "\"device\":\"%s\",\n"
            + "\"sensors\":\"%s\",\n"
            + "\"timestamp\":%d,\n"
            + "\"types\":\"%s\",\n"
            + "\"values\":\"%s\"\n"
            + "}",
        deviceId, sensors.toString(), timestamp, types.toString(), values.toString());
  }
}
