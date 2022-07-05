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

package org.apache.iceberg.flink.sink;

import java.math.BigDecimal;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestPartitionUdf {

  public static TableEnvironment tEnv;

  @BeforeClass
  public static void before() {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    tEnv = StreamTableEnvironment.create(env);
    tEnv.createTemporarySystemFunction("buckets", PartitionTransformUdf.Bucket.class);
    tEnv.createTemporarySystemFunction("truncates", PartitionTransformUdf.Truncate.class);
  }

  protected List<Row> sql(String query, Object... args) {
    TableResult tableResult = tEnv.executeSql(String.format(query, args));
    try (CloseableIterator<Row> it = tableResult.collect()) {
      return Lists.newArrayList(it);
    } catch (Exception e) {
      throw new RuntimeException("Failed to collect table result", e);
    }
  }

  public Object getSqlResult(String query, Object... args) {
    List<Row> ret = sql(query, args);
    return ret.get(0).getField(0);
  }
  @Test
  public void testBucket() {

    // int type
    Assert.assertEquals(428, getSqlResult("SELECT buckets(1000, 10)"));

    // long type
    Assert.assertEquals(525, getSqlResult("SELECT buckets(1000, 456294967296)"));

    // date type
    Assert.assertEquals(51, getSqlResult("SELECT buckets(1000, DATE '2022-05-20')"));

    // timestamp_ltz type
    Assert.assertEquals(483,
        getSqlResult("select buckets(1000, ts) from " +
            "(select cast(TIMESTAMP '2022-05-20 10:12:55.038194' as timestamp_ltz(6)) as ts)"));

    // timestamp type
    Assert.assertEquals(441,
        getSqlResult("select buckets(1000, ts) from " +
            "(select cast(TIMESTAMP '2022-05-20 10:12:55.038194' as timestamp(6)) as ts)"));

    // time type
    Assert.assertEquals(440, getSqlResult("SELECT buckets(1000, TIME '14:08:59')"));

    // string type
    Assert.assertEquals(489, getSqlResult("SELECT buckets(1000, 'this is a string')"));

    // decimal type
    Assert.assertEquals(825, getSqlResult("select buckets(1000, cast(6.12345 as decimal(6,5)))"));

    // binary type
    Assert.assertEquals(798, getSqlResult("SELECT buckets(1000, x'010203040506')"));

    // boolean type, unsupported
    AssertHelpers.assertThrows("unsupported boolean type",
        ValidationException.class,
        () -> sql("SELECT buckets(1000, true)"));
  }

  @Test
  public void testTruncate() {

    // int type
    Assert.assertEquals(10, getSqlResult("SELECT truncates(10, 15)"));

    // long type
    Assert.assertEquals(456294967000L, getSqlResult("SELECT truncates(1000, 456294967296)"));

    // string type
    Assert.assertEquals("this is a ", getSqlResult("SELECT truncates(10, 'this is a string')"));

    // decimal type
    Assert.assertEquals(BigDecimal.valueOf(612000, 5),
        getSqlResult("select truncates(1000, cast(6.12345 as decimal(6, 5)) )"));

    // binary type
    Assert.assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
        (byte[]) getSqlResult("SELECT truncates(10, x'0102030405060708090a0b0c0d0e0f')"));


    // timestamp type, unsupported
    AssertHelpers.assertThrows("unsupported timestamp type",
        ValidationException.class,
        () -> sql("SELECT truncates(10, TIMESTAMP '2022-05-20 10:12:55.038194')"));

    // date type, unsupported
    AssertHelpers.assertThrows("unsupported date type",
        ValidationException.class,
        () -> sql("SELECT truncates(10, DATE '%s')", "2022-05-20"));

    // time type, unsupported
    AssertHelpers.assertThrows("unsupported time type",
        ValidationException.class,
        () -> sql("SELECT truncates(10, TIME '%s')", "14:08:59"));

    // boolean type, unsupported
    AssertHelpers.assertThrows("unsupported boolean type",
        ValidationException.class,
        () -> sql("SELECT truncates(10, true)"));
  }
}
