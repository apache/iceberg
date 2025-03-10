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
package org.apache.iceberg.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;

public class TestTransformUtil {

  @ParameterizedTest
  @CsvSource({"0, 1970", "1, 1971", "10, 1980", "47, 2017", "100, 2070"})
  public void test_humanYear(int yearOrdinal, String expected) {
    String result = TransformUtil.humanYear(yearOrdinal);
    assertThat(result).isEqualTo(expected);
  }

  private static Stream<Arguments> humanYears() {
    return Stream.of(
        Arguments.of(0, "1970"),
        Arguments.of(1, "1971"),
        Arguments.of(10, "1980"),
        Arguments.of(47, "2017"),
        Arguments.of(100, "2070"));
  }

  @ParameterizedTest
  @CsvSource({"0, 1970-01", "1, 1970-02", "12, 1971-01", "658, 2024-11"})
  public void test_humanMonths(int monthOrdinal, String expected) {
    String result = TransformUtil.humanMonth(monthOrdinal);
    assertThat(result).isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource({"0, 1970-01-01", "1, 1970-01-02", "12, 1970-01-13", "20000, 2024-10-04"})
  public void test_humanDays(int dayOrdinal, String expected) {
    String result = TransformUtil.humanDay(dayOrdinal);
    assertThat(result).isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource({"0, 1970-01-01-00", "1, 1970-01-01-01", "12, 1970-01-01-12", "24, 1970-01-02-00"})
  public void test_humanHours(int hourOrdinal, String expected) {
    String result = TransformUtil.humanHour(hourOrdinal);
    assertThat(result).isEqualTo(expected);
  }
}
