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

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.IOException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestSparkSchemaUtil {
  private static final Schema TEST_SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));

  @Test
  public void testEstiamteSizeMaxValue() throws IOException {
    Assert.assertEquals(
        "estimateSize returns Long max value",
        Long.MAX_VALUE,
        SparkSchemaUtil.estimateSize(null, Long.MAX_VALUE));
  }

  @Test
  public void testEstiamteSizeWithOverflow() throws IOException {
    long tableSize =
        SparkSchemaUtil.estimateSize(SparkSchemaUtil.convert(TEST_SCHEMA), Long.MAX_VALUE - 1);
    Assert.assertEquals("estimateSize handles overflow", Long.MAX_VALUE, tableSize);
  }

  @Test
  public void testEstiamteSize() throws IOException {
    long tableSize = SparkSchemaUtil.estimateSize(SparkSchemaUtil.convert(TEST_SCHEMA), 1);
    Assert.assertEquals("estimateSize matches with expected approximation", 24, tableSize);
  }
}
