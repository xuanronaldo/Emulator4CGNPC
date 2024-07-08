/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.cgn.iot.benchmark.mode;

import com.cgn.iot.benchmark.client.DataClient;
import com.cgn.iot.benchmark.client.operation.Operation;
import com.cgn.iot.benchmark.conf.Config;
import com.cgn.iot.benchmark.conf.ConfigDescriptor;
import com.cgn.iot.benchmark.tsdb.DBConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class VerificationWriteMode extends BaseMode {
  private static final Config config = ConfigDescriptor.getInstance().getConfig();

  @Override
  protected boolean preCheck() {
    List<DBConfig> dbConfigs = new ArrayList<>();
    dbConfigs.add(config.getDbConfig());
    if (config.isIS_DOUBLE_WRITE()) {
      dbConfigs.add(config.getANOTHER_DBConfig());
    }
    if (config.isIS_DELETE_DATA() && (!cleanUpData(dbConfigs))) {
      return false;
    }
    if (config.isCREATE_SCHEMA() && (!registerSchema())) {
      return false;
    }
    return true;
  }

  @Override
  protected void postCheck() {
    finalMeasure(
        baseModeMeasurement,
        dataClients.stream().map(DataClient::getMeasurement),
        startTime,
        Collections.singletonList(Operation.INGESTION));
  }
}
