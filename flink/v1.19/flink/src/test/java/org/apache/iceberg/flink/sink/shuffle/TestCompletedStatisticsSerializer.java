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
import org.apache.iceberg.SortKey;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class TestCompletedStatisticsSerializer extends SerializerTestBase<CompletedStatistics> {

  @Override
  protected TypeSerializer<CompletedStatistics> createSerializer() {
    return Fixtures.COMPLETED_STATISTICS_SERIALIZER;
  }

  @Override
  protected int getLength() {
    return -1;
  }

  @Override
  protected Class<CompletedStatistics> getTypeClass() {
    return CompletedStatistics.class;
  }

  @Override
  protected CompletedStatistics[] getTestData() {

    return new CompletedStatistics[] {
      CompletedStatistics.fromKeyFrequency(
          1L, ImmutableMap.of(CHAR_KEYS.get("a"), 1L, CHAR_KEYS.get("b"), 2L)),
      CompletedStatistics.fromKeySamples(2L, new SortKey[] {CHAR_KEYS.get("a"), CHAR_KEYS.get("b")})
    };
  }
}
