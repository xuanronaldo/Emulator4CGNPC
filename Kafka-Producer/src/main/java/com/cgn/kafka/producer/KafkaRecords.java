package com.cgn.kafka.producer;

import java.util.List;

public class KafkaRecords {
  List<KafkaRecord> records;

  public KafkaRecords(List<KafkaRecord> records) {
    this.records = records;
  }

  public List<KafkaRecord> getRecords() {
    return records;
  }

  public void setRecords(List<KafkaRecord> records) {
    this.records = records;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[\n");
    for (KafkaRecord record : records) {
      sb.append(record.toString()).append(",\n");
    }
    if (records.size() > 0) {
      sb.setLength(sb.length() - 2); // Remove last comma
    }
    sb.append("\n]");
    return sb.toString();
  }
}
