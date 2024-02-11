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
package org.apache.iceberg.mr.hive;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.text.DateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.common.DynFields;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestHiveIcebergStorageHandlerTimezone {
  private static final Optional<ThreadLocal<DateFormat>> dateFormat =
      Optional.ofNullable(
          (ThreadLocal<DateFormat>)
              DynFields.builder()
                  .hiddenImpl(TimestampWritable.class, "threadLocalDateFormat")
                  .defaultAlwaysNull()
                  .buildStatic()
                  .get());

  private static final Optional<ThreadLocal<TimeZone>> localTimeZone =
      Optional.ofNullable(
          (ThreadLocal<TimeZone>)
              DynFields.builder()
                  .hiddenImpl(DateWritable.class, "LOCAL_TIMEZONE")
                  .defaultAlwaysNull()
                  .buildStatic()
                  .get());

  @Parameters(name = "timezone={0}")
  public static Collection<Object[]> parameters() {
    return ImmutableList.of(
        new String[] {"America/New_York"},
        new String[] {"Asia/Kolkata"},
        new String[] {"UTC/Greenwich"});
  }

  private static TestHiveShell shell;

  private TestTables testTables;

  @Parameter public String timezoneString;

  @TempDir public Path temp;

  @BeforeAll
  public static void beforeClass() {
    shell = HiveIcebergStorageHandlerTestUtils.shell();
  }

  @AfterAll
  public static void afterClass() throws Exception {
    shell.stop();
  }

  @BeforeEach
  public void before() throws IOException {
    TimeZone.setDefault(TimeZone.getTimeZone(timezoneString));

    // Magic to clean cached date format and local timezone for Hive where the default timezone is
    // used/stored in the
    // cached object
    dateFormat.ifPresent(ThreadLocal::remove);
    localTimeZone.ifPresent(ThreadLocal::remove);

    this.testTables =
        HiveIcebergStorageHandlerTestUtils.testTables(
            shell, TestTables.TestTableType.HIVE_CATALOG, temp);
    // Uses spark as an engine so we can detect if we unintentionally try to use any execution
    // engines
    HiveIcebergStorageHandlerTestUtils.init(shell, testTables, temp, "spark");
  }

  @AfterEach
  public void after() throws Exception {
    HiveIcebergStorageHandlerTestUtils.close(shell);
  }

  @TestTemplate
  public void testDateQuery() throws IOException {
    Schema dateSchema = new Schema(optional(1, "d_date", Types.DateType.get()));

    List<Record> records =
        TestHelper.RecordsBuilder.newInstance(dateSchema)
            .add(LocalDate.of(2020, 1, 21))
            .add(LocalDate.of(2020, 1, 24))
            .build();

    testTables.createTable(shell, "date_test", dateSchema, FileFormat.PARQUET, records);

    List<Object[]> result =
        shell.executeStatement("SELECT * from date_test WHERE d_date='2020-01-21'");
    assertThat(result).hasSize(1);
    assertThat(result.get(0)[0]).isEqualTo("2020-01-21");

    result =
        shell.executeStatement(
            "SELECT * from date_test WHERE d_date in ('2020-01-21', '2020-01-22')");
    assertThat(result).hasSize(1);
    assertThat(result.get(0)[0]).isEqualTo("2020-01-21");

    result = shell.executeStatement("SELECT * from date_test WHERE d_date > '2020-01-21'");
    assertThat(result).hasSize(1);
    assertThat(result.get(0)[0]).isEqualTo("2020-01-24");

    result = shell.executeStatement("SELECT * from date_test WHERE d_date='2020-01-20'");
    assertThat(result).isEmpty();
  }

  @TestTemplate
  public void testTimestampQuery() throws IOException {
    Schema timestampSchema = new Schema(optional(1, "d_ts", Types.TimestampType.withoutZone()));

    List<Record> records =
        TestHelper.RecordsBuilder.newInstance(timestampSchema)
            .add(LocalDateTime.of(2019, 1, 22, 9, 44, 54, 100000000))
            .add(LocalDateTime.of(2019, 2, 22, 9, 44, 54, 200000000))
            .build();

    testTables.createTable(shell, "ts_test", timestampSchema, FileFormat.PARQUET, records);

    List<Object[]> result =
        shell.executeStatement("SELECT d_ts FROM ts_test WHERE d_ts='2019-02-22 09:44:54.2'");
    assertThat(result).hasSize(1);
    assertThat(result.get(0)[0]).isEqualTo("2019-02-22 09:44:54.2");

    result =
        shell.executeStatement(
            "SELECT * FROM ts_test WHERE d_ts in ('2017-01-01 22:30:57.1', '2019-02-22 09:44:54.2')");
    assertThat(result).hasSize(1);
    assertThat(result.get(0)[0]).isEqualTo("2019-02-22 09:44:54.2");

    result =
        shell.executeStatement("SELECT d_ts FROM ts_test WHERE d_ts < '2019-02-22 09:44:54.2'");
    assertThat(result).hasSize(1);
    assertThat(result.get(0)[0]).isEqualTo("2019-01-22 09:44:54.1");

    result = shell.executeStatement("SELECT * FROM ts_test WHERE d_ts='2017-01-01 22:30:57.3'");
    assertThat(result).isEmpty();
  }
}
