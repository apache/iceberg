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

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

public class DateTimeUtil {
  private static final LocalDate EPOCH_DAY = LocalDate.of(1970, 1, 1);

  private DateTimeUtil() {}

  public static int hours(String isoTime) {
    long micros = org.apache.iceberg.util.DateTimeUtil.isoTimestampToMicros(isoTime);
    return org.apache.iceberg.util.DateTimeUtil.microsToHours(micros);
  }

  public static int days(String isoDate) {
    return (int) ChronoUnit.DAYS.between(EPOCH_DAY, LocalDate.parse(isoDate));
  }

  public static int months(String isoDate) {
    return (int) ChronoUnit.MONTHS.between(EPOCH_DAY, LocalDate.parse(isoDate));
  }

  public static int years(String isoDate) {
    return (int) ChronoUnit.YEARS.between(EPOCH_DAY, LocalDate.parse(isoDate));
  }
}
