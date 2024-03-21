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
package org.apache.iceberg;

import static org.apache.iceberg.ScanSummary.timestampRange;
import static org.apache.iceberg.ScanSummary.toMillis;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestScanSummary extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2);
  }

  @TestTemplate
  public void testSnapshotTimeRangeValidation() {
    long t0 = System.currentTimeMillis();

    table
        .newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    long t1 = System.currentTimeMillis();
    while (t1 <= table.currentSnapshot().timestampMillis()) {
      t1 = System.currentTimeMillis();
    }

    table
        .newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    long secondSnapshotId = table.currentSnapshot().snapshotId();

    long t2 = System.currentTimeMillis();
    while (t2 <= table.currentSnapshot().timestampMillis()) {
      t2 = System.currentTimeMillis();
    }

    // expire the first snapshot
    table.expireSnapshots().expireOlderThan(t1).commit();

    assertThat(table.snapshots()).hasSize(1);
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(secondSnapshotId);

    // this should include the first snapshot, but it was removed from the dataset
    TableScan scan =
        table
            .newScan()
            .filter(greaterThanOrEqual("dateCreated", t0))
            .filter(lessThan("dateCreated", t2));

    assertThatThrownBy(() -> new ScanSummary.Builder(scan).build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot satisfy time filters: time range may include expired snapshots");
  }

  @TestTemplate
  public void testTimestampRanges() {
    long lower = 1542750188523L;
    long upper = 1542750695131L;

    assertThat(timestampRange(ImmutableList.of(lessThanOrEqual("ts_ms", upper))))
        .isEqualTo(Pair.of(Long.MIN_VALUE, upper));

    assertThat(
            timestampRange(
                ImmutableList.of(
                    lessThanOrEqual("ts_ms", upper + 918234), lessThanOrEqual("ts_ms", upper))))
        .as("Should use lower value for upper bound")
        .isEqualTo(Pair.of(Long.MIN_VALUE, upper));

    assertThat(timestampRange(ImmutableList.of(lessThan("ts_ms", upper))))
        .as("Should make upper bound inclusive")
        .isEqualTo(Pair.of(Long.MIN_VALUE, upper - 1));

    assertThat(timestampRange(ImmutableList.of(greaterThanOrEqual("ts_ms", lower))))
        .isEqualTo(Pair.of(lower, Long.MAX_VALUE));

    assertThat(
            timestampRange(
                ImmutableList.of(
                    greaterThanOrEqual("ts_ms", lower - 918234),
                    greaterThanOrEqual("ts_ms", lower))))
        .as("Should use upper value for lower bound")
        .isEqualTo(Pair.of(lower, Long.MAX_VALUE));

    assertThat(timestampRange(ImmutableList.of(greaterThan("ts_ms", lower))))
        .as("Should make lower bound inclusive")
        .isEqualTo(Pair.of(lower + 1, Long.MAX_VALUE));

    assertThat(timestampRange(ImmutableList.of(equal("ts_ms", lower))))
        .isEqualTo(Pair.of(lower, lower));

    assertThat(
            timestampRange(
                ImmutableList.of(greaterThanOrEqual("ts_ms", lower), lessThan("ts_ms", upper))))
        .as("Should set both bounds and make upper bound inclusive")
        .isEqualTo(Pair.of(lower, upper - 1));

    // >= lower and < lower is an empty range
    assertThatThrownBy(
            () ->
                timestampRange(
                    ImmutableList.of(greaterThanOrEqual("ts_ms", lower), lessThan("ts_ms", lower))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("No timestamps can match filters");
  }

  @TestTemplate
  public void testToMillis() {
    long millis = 1542750947417L;
    assertThat(toMillis(millis / 1000)).isEqualTo(1542750947000L);
    assertThat(toMillis(millis)).isEqualTo(1542750947417L);
    assertThat(toMillis(millis * 1000 + 918)).isEqualTo(1542750947417L);
  }
}
