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
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestDateTimeUtil {
  @Test
  public void nanosToMicros() {
    assertThat(DateTimeUtil.nanosToMicros(1510871468000001001L)).isEqualTo(1510871468000001L);
    assertThat(DateTimeUtil.nanosToMicros(-1510871468000001001L)).isEqualTo(-1510871468000002L);
  }

  @Test
  public void microsToNanos() {
    assertThat(DateTimeUtil.microsToNanos(1510871468000001L)).isEqualTo(1510871468000001000L);
    assertThat(DateTimeUtil.microsToNanos(-1510871468000001L)).isEqualTo(-1510871468000001000L);
  }

  @Test
  public void isoTimestampToNanos() {
    assertThat(DateTimeUtil.isoTimestampToNanos("2017-11-16T22:31:08.000001001"))
        .isEqualTo(1510871468000001001L);
    assertThat(DateTimeUtil.isoTimestampToNanos("1922-02-15T01:28:51.999998999"))
        .isEqualTo(-1510871468000001001L);
  }

  @Test
  public void isoTimestamptzToNanos() {
    assertThat(DateTimeUtil.isoTimestamptzToNanos("2017-11-16T14:31:08.000001001-08:00"))
        .isEqualTo(1510871468000001001L);
    assertThat(DateTimeUtil.isoTimestamptzToNanos("1922-02-15T01:28:51.999998999+00:00"))
        .isEqualTo(-1510871468000001001L);
  }

  @Test
  public void convertNanos() {
    assertThat(
            Transforms.identity()
                .toHumanString(Types.TimestampNanoType.withoutZone(), 1510871468000001001L))
        .isEqualTo("2017-11-16T22:31:08.000001001");
    assertThat(DateTimeUtil.nanosToYears(1510871468000001001L)).isEqualTo(47);
    assertThat(Transforms.year().toHumanString(Types.IntegerType.get(), 47)).isEqualTo("2017");
    assertThat(DateTimeUtil.nanosToMonths(1510871468000001001L)).isEqualTo(574);
    assertThat(Transforms.month().toHumanString(Types.IntegerType.get(), 574)).isEqualTo("2017-11");
    assertThat(DateTimeUtil.nanosToDays(1510871468000001001L)).isEqualTo(17486);
    assertThat(Transforms.day().toHumanString(Types.IntegerType.get(), 17486))
        .isEqualTo("2017-11-16");
    assertThat(DateTimeUtil.nanosToHours(1510871468000001001L)).isEqualTo(419686);
    assertThat(Transforms.hour().toHumanString(Types.IntegerType.get(), 419686))
        .isEqualTo("2017-11-16-22");
  }

  @Test
  public void convertNanosNegative() {
    assertThat(
            Transforms.identity()
                .toHumanString(Types.TimestampNanoType.withZone(), -1510871468000001001L))
        .isEqualTo("1922-02-15T01:28:51.999998999+00:00");
    assertThat(DateTimeUtil.nanosToYears(-1510871468000001001L)).isEqualTo(-48);
    assertThat(Transforms.year().toHumanString(Types.IntegerType.get(), 47)).isEqualTo("2017");
    assertThat(DateTimeUtil.nanosToMonths(-1510871468000001001L)).isEqualTo(-575);
    assertThat(Transforms.month().toHumanString(Types.IntegerType.get(), 574)).isEqualTo("2017-11");
    assertThat(DateTimeUtil.nanosToDays(-1510871468000001001L)).isEqualTo(-17487);
    assertThat(Transforms.day().toHumanString(Types.IntegerType.get(), 17486))
        .isEqualTo("2017-11-16");
    assertThat(DateTimeUtil.nanosToHours(-1510871468000001001L)).isEqualTo(-419687);
    assertThat(Transforms.hour().toHumanString(Types.IntegerType.get(), 419686))
        .isEqualTo("2017-11-16-22");
  }
}
