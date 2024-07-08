package com.cgn.kafka.consumer;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

public class KafkaRecord {
  String deviceId;
  Long timestamp;
  List<String> measurements;
  List<TSDataType> types;
  List<Object> values;
  List<String> sensors;

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

  public String getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  public List<String> getMeasurements() {
    return measurements;
  }

  public void setMeasurements(List<String> measurements) {
    this.measurements = measurements;
  }

  public List<TSDataType> getTypes() {
    return types;
  }

  public void setTypes(List<TSDataType> types) {
    this.types = types;
  }

  public List<Object> getValues() {
    return values;
  }

  public void setValues(List<Object> values) {
    this.values = values;
  }

  public List<String> getSensors() {
    return sensors;
  }

  public void setSensors(List<String> sensors) {
    this.sensors = sensors;
  }

  @Override
  public String toString() {
    return String.format(
        "{\n"
            + "\"device\":\"%s\",\n"
            + "\"sensors\":%s\n"
            + "\"timestamp\":%d,\n"
            + "\"types\":%s,\n"
            + "\"values\":%s,\n"
            + "}",
        deviceId, sensors.toString(), timestamp, types.toString(), values.toString());
  }
}
