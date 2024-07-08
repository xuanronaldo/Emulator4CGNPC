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

package com.cgn.iot.benchmark.tsdb.fakedb;

import com.cgn.iot.benchmark.entity.Batch.IBatch;
import com.cgn.iot.benchmark.measurement.Status;
import com.cgn.iot.benchmark.schema.schemaImpl.DeviceSchema;
import com.cgn.iot.benchmark.tsdb.IDatabase;
import com.cgn.iot.benchmark.tsdb.TsdbException;
import com.cgn.iot.benchmark.workload.query.impl.AggRangeQuery;
import com.cgn.iot.benchmark.workload.query.impl.AggRangeValueQuery;
import com.cgn.iot.benchmark.workload.query.impl.AggValueQuery;
import com.cgn.iot.benchmark.workload.query.impl.GroupByQuery;
import com.cgn.iot.benchmark.workload.query.impl.LatestPointQuery;
import com.cgn.iot.benchmark.workload.query.impl.PreciseQuery;
import com.cgn.iot.benchmark.workload.query.impl.RangeQuery;
import com.cgn.iot.benchmark.workload.query.impl.ValueRangeQuery;

import java.util.List;

public class FakeDB implements IDatabase {

  @Override
  public void init() throws TsdbException {}

  @Override
  public void cleanup() throws TsdbException {}

  @Override
  public void close() throws TsdbException {}

  @Override
  public Double registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    return 0.0;
  }

  @Override
  public Status insertOneBatch(IBatch batch) {
    return new Status(true);
  }

  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    return new Status(true, null, null);
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    return new Status(true, null, null);
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    return new Status(true, 0);
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    return new Status(true, null, null);
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    return new Status(true, null, null);
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    return new Status(true, null, null);
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    return new Status(true, null, null);
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    return new Status(true, null, null);
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
