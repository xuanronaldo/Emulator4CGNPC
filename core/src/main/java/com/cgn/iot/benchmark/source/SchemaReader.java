package com.cgn.iot.benchmark.source;

import com.cgn.iot.benchmark.conf.Config;
import com.cgn.iot.benchmark.conf.ConfigDescriptor;
import com.cgn.iot.benchmark.entity.Sensor;

import java.util.List;
import java.util.Map;

public abstract class SchemaReader {

  protected static final Config config = ConfigDescriptor.getInstance().getConfig();

  /**
   * get device schema based on file name and data set sensorType
   *
   * @return device schema list to register
   */
  public abstract Map<String, List<Sensor>> getDeviceSchemaList();

  /**
   * Check whether dataset is valid
   *
   * @return result
   */
  public abstract boolean checkDataSet();
}
