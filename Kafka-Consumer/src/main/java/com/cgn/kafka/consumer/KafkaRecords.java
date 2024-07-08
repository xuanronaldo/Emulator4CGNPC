package com.cgn.kafka.consumer;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

public class KafkaRecords {
  List<String> devices = new ArrayList<>();
  List<Long> times = new ArrayList<>();
  List<List<String>> measurementsList = new ArrayList<>();
  List<List<TSDataType>> tyoesList = new ArrayList<>();
  List<List<Object>> valuesList = new ArrayList<>();

  public List<String> getDevices() {
    return devices;
  }

  public void setDevices(List<String> devices) {
    this.devices = devices;
  }

  public List<Long> getTimes() {
    return times;
  }

  public void setTimes(List<Long> times) {
    this.times = times;
  }

  public List<List<String>> getMeasurementsList() {
    return measurementsList;
  }

  public void setMeasurementsList(List<List<String>> measurementsList) {
    this.measurementsList = measurementsList;
  }

  public List<List<TSDataType>> getTyoesList() {
    return tyoesList;
  }

  public void setTyoesList(List<List<TSDataType>> tyoesList) {
    this.tyoesList = tyoesList;
  }

  public List<List<Object>> getValuesList() {
    return valuesList;
  }

  public void setValuesList(List<List<Object>> valuesList) {
    this.valuesList = valuesList;
  }
}
