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
package org.apache.iceberg.spark;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.KryoHelpers;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkTableUtil.SparkPartition;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestSparkTableUtil {
  @Test
  public void testSparkPartitionOKryoSerialization() throws IOException {
    Map<String, String> values = ImmutableMap.of("id", "2");
    String uri = "s3://bucket/table/data/id=2";
    String format = "parquet";
    SparkPartition sparkPartition = new SparkPartition(values, uri, format);

    SparkPartition deserialized = KryoHelpers.roundTripSerialize(sparkPartition);
    Assertions.assertThat(sparkPartition).isEqualTo(deserialized);
  }

  @Test
  public void testSparkPartitionJavaSerialization() throws IOException, ClassNotFoundException {
    Map<String, String> values = ImmutableMap.of("id", "2");
    String uri = "s3://bucket/table/data/id=2";
    String format = "parquet";
    SparkPartition sparkPartition = new SparkPartition(values, uri, format);

    SparkPartition deserialized = TestHelpers.roundTripSerialize(sparkPartition);
    Assertions.assertThat(sparkPartition).isEqualTo(deserialized);
  }
}
