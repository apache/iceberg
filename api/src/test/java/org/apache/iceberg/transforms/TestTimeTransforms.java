/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestTimeTransforms {
  @Test
  public void testMicrosSatisfiesOrderOfDates() {
    assertThat(Hours.get().satisfiesOrderOf(Dates.DAY)).isTrue();
    assertThat(Hours.get().satisfiesOrderOf(Dates.MONTH)).isTrue();
    assertThat(Hours.get().satisfiesOrderOf(Dates.YEAR)).isTrue();

    assertThat(Days.get().satisfiesOrderOf(Dates.DAY)).isTrue();
    assertThat(Days.get().satisfiesOrderOf(Dates.MONTH)).isTrue();
    assertThat(Days.get().satisfiesOrderOf(Dates.YEAR)).isTrue();

    assertThat(Months.get().satisfiesOrderOf(Dates.DAY)).isFalse();
    assertThat(Months.get().satisfiesOrderOf(Dates.MONTH)).isTrue();
    assertThat(Months.get().satisfiesOrderOf(Dates.YEAR)).isTrue();

    assertThat(Years.get().satisfiesOrderOf(Dates.DAY)).isFalse();
    assertThat(Years.get().satisfiesOrderOf(Dates.MONTH)).isFalse();
    assertThat(Years.get().satisfiesOrderOf(Dates.YEAR)).isTrue();
  }

  @Test
  public void testMicrosSatisfiesOrderOfTimestamps() {
    assertThat(Hours.get().satisfiesOrderOf(Timestamps.MICROS_TO_HOUR)).isTrue();
    assertThat(Hours.get().satisfiesOrderOf(Timestamps.MICROS_TO_DAY)).isTrue();
    assertThat(Hours.get().satisfiesOrderOf(Timestamps.MICROS_TO_MONTH)).isTrue();
    assertThat(Hours.get().satisfiesOrderOf(Timestamps.MICROS_TO_YEAR)).isTrue();

    assertThat(Days.get().satisfiesOrderOf(Timestamps.MICROS_TO_HOUR)).isFalse();
    assertThat(Days.get().satisfiesOrderOf(Timestamps.MICROS_TO_DAY)).isTrue();
    assertThat(Days.get().satisfiesOrderOf(Timestamps.MICROS_TO_MONTH)).isTrue();
    assertThat(Days.get().satisfiesOrderOf(Timestamps.MICROS_TO_YEAR)).isTrue();

    assertThat(Months.get().satisfiesOrderOf(Timestamps.MICROS_TO_HOUR)).isFalse();
    assertThat(Months.get().satisfiesOrderOf(Timestamps.MICROS_TO_DAY)).isFalse();
    assertThat(Months.get().satisfiesOrderOf(Timestamps.MICROS_TO_MONTH)).isTrue();
    assertThat(Months.get().satisfiesOrderOf(Timestamps.MICROS_TO_YEAR)).isTrue();

    assertThat(Years.get().satisfiesOrderOf(Timestamps.MICROS_TO_HOUR)).isFalse();
    assertThat(Years.get().satisfiesOrderOf(Timestamps.MICROS_TO_DAY)).isFalse();
    assertThat(Years.get().satisfiesOrderOf(Timestamps.MICROS_TO_MONTH)).isFalse();
    assertThat(Years.get().satisfiesOrderOf(Timestamps.MICROS_TO_YEAR)).isTrue();
  }

  @Test
  public void testMicrosSatisfiesOrderOfTimestampNanos() {
    assertThat(Hours.get().satisfiesOrderOf(Timestamps.NANOS_TO_HOUR)).isTrue();
    assertThat(Hours.get().satisfiesOrderOf(Timestamps.NANOS_TO_DAY)).isTrue();
    assertThat(Hours.get().satisfiesOrderOf(Timestamps.NANOS_TO_MONTH)).isTrue();
    assertThat(Hours.get().satisfiesOrderOf(Timestamps.NANOS_TO_YEAR)).isTrue();

    assertThat(Days.get().satisfiesOrderOf(Timestamps.NANOS_TO_HOUR)).isFalse();
    assertThat(Days.get().satisfiesOrderOf(Timestamps.NANOS_TO_DAY)).isTrue();
    assertThat(Days.get().satisfiesOrderOf(Timestamps.NANOS_TO_MONTH)).isTrue();
    assertThat(Days.get().satisfiesOrderOf(Timestamps.NANOS_TO_YEAR)).isTrue();

    assertThat(Months.get().satisfiesOrderOf(Timestamps.NANOS_TO_HOUR)).isFalse();
    assertThat(Months.get().satisfiesOrderOf(Timestamps.NANOS_TO_DAY)).isFalse();
    assertThat(Months.get().satisfiesOrderOf(Timestamps.NANOS_TO_MONTH)).isTrue();
    assertThat(Months.get().satisfiesOrderOf(Timestamps.NANOS_TO_YEAR)).isTrue();

    assertThat(Years.get().satisfiesOrderOf(Timestamps.NANOS_TO_HOUR)).isFalse();
    assertThat(Years.get().satisfiesOrderOf(Timestamps.NANOS_TO_DAY)).isFalse();
    assertThat(Years.get().satisfiesOrderOf(Timestamps.NANOS_TO_MONTH)).isFalse();
    assertThat(Years.get().satisfiesOrderOf(Timestamps.NANOS_TO_YEAR)).isTrue();
  }

  @Test
  public void testMicrosSatisfiesOrderOfTimeTransforms() {
    assertThat(Hours.get().satisfiesOrderOf(Hours.get())).isTrue();
    assertThat(Hours.get().satisfiesOrderOf(Days.get())).isTrue();
    assertThat(Hours.get().satisfiesOrderOf(Months.get())).isTrue();
    assertThat(Hours.get().satisfiesOrderOf(Years.get())).isTrue();

    assertThat(Days.get().satisfiesOrderOf(Hours.get())).isFalse();
    assertThat(Days.get().satisfiesOrderOf(Days.get())).isTrue();
    assertThat(Days.get().satisfiesOrderOf(Months.get())).isTrue();
    assertThat(Days.get().satisfiesOrderOf(Years.get())).isTrue();

    assertThat(Months.get().satisfiesOrderOf(Hours.get())).isFalse();
    assertThat(Months.get().satisfiesOrderOf(Days.get())).isFalse();
    assertThat(Months.get().satisfiesOrderOf(Months.get())).isTrue();
    assertThat(Months.get().satisfiesOrderOf(Years.get())).isTrue();

    assertThat(Years.get().satisfiesOrderOf(Hours.get())).isFalse();
    assertThat(Years.get().satisfiesOrderOf(Days.get())).isFalse();
    assertThat(Years.get().satisfiesOrderOf(Months.get())).isFalse();
    assertThat(Years.get().satisfiesOrderOf(Years.get())).isTrue();
  }

  @Test
  public void testHoursToEnum() {
    Hours<Object> hours = Hours.get();
    Types.DateType type = Types.DateType.get();
    assertThatThrownBy(() -> hours.toEnum(type))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageMatching("Unsupported type: date");
  }

}
