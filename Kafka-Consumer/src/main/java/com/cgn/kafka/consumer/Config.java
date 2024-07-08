package com.cgn.kafka.consumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config {

  private Properties properties;

  public Config(String fileName) {
    properties = new Properties();
    try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName)) {
      if (inputStream == null) {
        System.out.println("unable find file");
        return;
      }
      properties.load(inputStream);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public String getProperty(String key) {
    String value = (String) properties.get(key);
    if (value == null) {
      return null;
    }
    return value.trim();
  }
}
