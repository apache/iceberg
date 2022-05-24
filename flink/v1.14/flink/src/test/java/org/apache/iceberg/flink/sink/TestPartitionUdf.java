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
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
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
    tEnv.createTemporarySystemFunction("years", PartitionTransformUdf.Year.class);
    tEnv.createTemporarySystemFunction("months", PartitionTransformUdf.Month.class);
    tEnv.createTemporarySystemFunction("days", PartitionTransformUdf.Day.class);
    tEnv.createTemporarySystemFunction("hours", PartitionTransformUdf.Hour.class);
  }

  protected List<Row> sql(String query, Object... args) {
    TableResult tableResult = tEnv.executeSql(String.format(query, args));
    try (CloseableIterator<Row> it = tableResult.collect()) {
      return Lists.newArrayList(it);
    } catch (Exception e) {
      throw new RuntimeException("Failed to collect table result", e);
    }
  }


  @Test
  public void testYears() {
    Transform<Object, Integer> yearDate = Transforms.year(Types.DateType.get());
    String date = "2022-05-20";
    Integer value = yearDate.apply(Literal.of(date).to(Types.DateType.get()).value());
    String expectHumanString = yearDate.toHumanString(value);
    List<Row> sql = sql("SELECT years(DATE '%s')", date);
    Assert.assertEquals(expectHumanString, sql.get(0).getField(0));

    yearDate = Transforms.year(Types.TimestampType.withoutZone());
    String ts = "2022-05-20T10:12:55.038194";
    Literal<Long> lts = Literal.of(ts).to((Types.TimestampType.withoutZone()));
    value = yearDate.apply(lts.value());
    expectHumanString = yearDate.toHumanString(value);
    sql = sql("SELECT years(TIMESTAMP '2022-05-20 10:12:55.038194')");
    Assert.assertEquals(expectHumanString, sql.get(0).getField(0));
  }

  @Test
  public void testMonths() {
    Transform<Object, Integer> monthDate = Transforms.month(Types.DateType.get());
    String date = "2022-05-20";
    Integer value = monthDate.apply(Literal.of(date).to(Types.DateType.get()).value());
    String expectHumanString = monthDate.toHumanString(value);
    List<Row> sql = sql("SELECT months(DATE '%s')", date);
    Assert.assertEquals(expectHumanString, sql.get(0).getField(0));

    monthDate = Transforms.month(Types.TimestampType.withoutZone());
    String ts = "2022-05-20T10:12:55.038194";
    Literal<Long> lts = Literal.of(ts).to((Types.TimestampType.withoutZone()));
    value = monthDate.apply(lts.value());
    expectHumanString = monthDate.toHumanString(value);
    sql = sql("SELECT months(TIMESTAMP '2022-05-20 10:12:55.038194')");
    Assert.assertEquals(expectHumanString, sql.get(0).getField(0));
  }

  @Test
  public void testDays() {
    Transform<Object, Integer> dayDate = Transforms.day(Types.DateType.get());
    String date = "2022-05-20";
    Integer value = dayDate.apply(Literal.of(date).to(Types.DateType.get()).value());
    String expectHumanString = dayDate.toHumanString(value);
    List<Row> sql = sql("SELECT days(DATE '%s')", date);
    Assert.assertEquals(expectHumanString, sql.get(0).getField(0));

    dayDate = Transforms.day(Types.TimestampType.withoutZone());
    String ts = "2022-05-20T10:12:55.038194";
    Literal<Long> lts = Literal.of(ts).to((Types.TimestampType.withoutZone()));
    value = dayDate.apply(lts.value());
    expectHumanString = dayDate.toHumanString(value);
    sql = sql("SELECT days(TIMESTAMP '2022-05-20 10:12:55.038194')");
    Assert.assertEquals(expectHumanString, sql.get(0).getField(0));
  }

  @Test
  public void testHours() {
    Transform<Object, Integer> hourDate = Transforms.hour(Types.TimestampType.withoutZone());
    String ts = "2022-05-20T10:12:55.038194";
    Literal<Long> lts = Literal.of(ts).to((Types.TimestampType.withoutZone()));
    Integer value = hourDate.apply(lts.value());
    String expectHumanString = hourDate.toHumanString(value);
    List<Row> sql = sql("SELECT hours(TIMESTAMP '2022-05-20 10:12:55.038194')");
    Assert.assertEquals(expectHumanString, sql.get(0).getField(0));
  }

  @Test
  public void testBucketDate() {
    Transform<Object, Integer> bucket = Transforms.bucket(Types.DateType.get(), 4);

    String date = "2022-05-20";
    Integer value = bucket.apply(Literal.of(date).to(Types.DateType.get()).value());
    String expectHumanString = bucket.toHumanString(value);

    List<Row> sql = sql("SELECT buckets(4, DATE '%s')", date);
    Assert.assertEquals(expectHumanString, sql.get(0).getField(0));
  }

  @Test
  public void testBucketInt() {
    Transform<Object, Integer> bucket = Transforms.bucket(Types.IntegerType.get(), 4);

    int num = 10;
    Integer obj = bucket.apply(num);
    String expectHumanString = bucket.toHumanString(obj);

    List<Row> sql = sql("SELECT buckets(4, %d)", num);
    Assert.assertEquals(expectHumanString, sql.get(0).getField(0));
  }

  @Test
  public void testBucketLong() {
    Transform<Object, Integer> bucket = Transforms.bucket(Types.LongType.get(), 4);

    long num = 10;
    Integer obj = bucket.apply(num);
    String expectHumanString = bucket.toHumanString(obj);

    List<Row> sql = sql("SELECT buckets(4, %d)", num);
    Assert.assertEquals(expectHumanString, sql.get(0).getField(0));
  }

  @Test
  public void testBucketTimestamp() {
    Transform<Object, Integer> bucket = Transforms.bucket(Types.TimestampType.withoutZone(), 4);
    String ts = "2022-05-20T10:12:55.038194";
    Literal<Long> lts = Literal.of(ts).to((Types.TimestampType.withoutZone()));
    Integer apply = bucket.apply(lts.value());
    String expectHumanString = bucket.toHumanString(apply);

    List<Row> sql = sql("SELECT buckets(4, TIMESTAMP '2022-05-20 10:12:55.038194')");
    Assert.assertEquals(expectHumanString, sql.get(0).getField(0));
  }

  @Test
  public void testBucketString() {
    Transform<Object, Integer> bucket = Transforms.bucket(Types.StringType.get(), 4);

    String str = "abcdef";
    Integer obj = bucket.apply(str);
    String expectHumanString = bucket.toHumanString(obj);

    List<Row> sql = sql("SELECT buckets(4, '%s')", str);
    Assert.assertEquals(expectHumanString, sql.get(0).getField(0));
  }

  @Test
  public void testBucketDecimalType() {
    Transform<Object, Integer> bucket = Transforms.bucket(Types.DecimalType.of(6, 5), 4);

    Integer obj = bucket.apply(BigDecimal.valueOf(6.12345));
    String expectHumanString = bucket.toHumanString(obj);

    List<Row> sql = sql("select buckets(4, cast(6.12345 as decimal(6,5)))");
    Assert.assertEquals(expectHumanString, sql.get(0).getField(0));
  }

  @Test
  public void testBucketFixed() {
    Transform<Object, Integer> bucket = Transforms.bucket(Types.FixedType.ofLength(6), 4);

    Integer obj = bucket.apply(ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5, 6}));
    String expectHumanString = bucket.toHumanString(obj);

    List<Row> sql = sql("SELECT buckets(4, x'010203040506')");
    Assert.assertEquals(expectHumanString, sql.get(0).getField(0));
  }

  @Test
  public void testBucketTime() {
    Transform<Object, Integer> bucket = Transforms.bucket(Types.TimeType.get(), 4);

    String str = "14:08:59";
    Integer obj = bucket.apply(Literal.of(str).to(Types.TimeType.get()).value());
    String expectHumanString = bucket.toHumanString(obj);

    List<Row> sql = sql("SELECT buckets(4, TIME '%s')", str);
    Assert.assertEquals(expectHumanString, sql.get(0).getField(0));
  }

  @Test
  public void testBucketByteBuffer() {
    Transform<Object, Integer> bucket = Transforms.bucket(Types.BinaryType.get(), 4);

    Integer obj = bucket.apply(ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5, 6}));
    String expectHumanString = bucket.toHumanString(obj);

    List<Row> sql = sql("SELECT buckets(4, x'010203040506')");
    Assert.assertEquals(expectHumanString, sql.get(0).getField(0));
  }

  @Test
  public void testTruncateInt() {
    Transform<Object, Object> truncate = Transforms.truncate(Types.IntegerType.get(), 4);

    int num = 10;
    Object obj = truncate.apply(num);
    String expectHumanString = truncate.toHumanString(obj);

    List<Row> sql = sql("SELECT truncates(4, %d)", num);
    Assert.assertEquals(expectHumanString, sql.get(0).getField(0));
  }

  @Test
  public void testTruncateLong() {
    Transform<Object, Object> truncate = Transforms.truncate(Types.LongType.get(), 4);

    long num = 10;
    Object obj = truncate.apply(num);
    String expectHumanString = truncate.toHumanString(obj);

    List<Row> sql = sql("SELECT truncates(4, %d)", num);
    Assert.assertEquals(expectHumanString, sql.get(0).getField(0));
  }

  @Test
  public void testTruncateString() {
    Transform<Object, Object> truncate = Transforms.truncate(Types.StringType.get(), 4);

    String str = "abcdef";
    Object obj = truncate.apply(str);
    String expectHumanString = truncate.toHumanString(obj);

    List<Row> sql = sql("SELECT truncates(4, '%s')", str);
    Assert.assertEquals(expectHumanString, sql.get(0).getField(0));
  }

  @Test
  public void testTruncateDecimalType() {
    Transform<Object, Object> truncate = Transforms.truncate(Types.DecimalType.of(6, 5), 4);

    Object obj = truncate.apply(BigDecimal.valueOf(6.12345));
    String expectHumanString = truncate.toHumanString(obj);

    List<Row> sql = sql("select truncates(4, cast(6.12345 as decimal(6,5)))");
    Assert.assertEquals(expectHumanString, sql.get(0).getField(0));
  }

  @Test
  public void testTruncateByteBuffer() {
    Transform<Object, Object> truncate = Transforms.truncate(Types.BinaryType.get(), 4);

    Object obj = truncate.apply(ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5, 6}));
    String expectHumanString = truncate.toHumanString(obj);

    List<Row> sql = sql("SELECT truncates(4, x'010203040506')");
    Assert.assertEquals(expectHumanString, sql.get(0).getField(0));
  }

  @Test
  public void testCreatePartitionTable() {

  }

}
