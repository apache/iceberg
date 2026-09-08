/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.sink.shuffle;

import static org.apache.iceberg.flink.sink.shuffle.Fixtures.CHAR_KEYS;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;

public class TestDataStatisticsSerializer extends SerializerTestBase<DataStatistics> {
  @Override
  protected TypeSerializer<DataStatistics> createSerializer() {
    return Fixtures.TASK_STATISTICS_SERIALIZER;
  }

  @Override
  protected int getLength() {
    return -1;
  }

  @Override
  protected Class<DataStatistics> getTypeClass() {
    return DataStatistics.class;
  }

  @Override
  protected DataStatistics[] getTestData() {
    return new DataStatistics[] {
      new MapDataStatistics(),
      Fixtures.createTaskStatistics(
          StatisticsType.Map, CHAR_KEYS.get("a"), CHAR_KEYS.get("a"), CHAR_KEYS.get("b")),
      new SketchDataStatistics(128),
      Fixtures.createTaskStatistics(
          StatisticsType.Sketch, CHAR_KEYS.get("a"), CHAR_KEYS.get("a"), CHAR_KEYS.get("b"))
    };
  }
}
