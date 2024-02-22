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

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

// copied from internal JavaUtils in Spark, not accessible in 3.4
class JavaUtils {

  private static final Map<String, TimeUnit> TIME_SUFFIXES =
      ImmutableMap.<String, TimeUnit>builder()
          .put("us", TimeUnit.MICROSECONDS)
          .put("ms", TimeUnit.MILLISECONDS)
          .put("s", TimeUnit.SECONDS)
          .put("m", TimeUnit.MINUTES)
          .put("min", TimeUnit.MINUTES)
          .put("h", TimeUnit.HOURS)
          .put("d", TimeUnit.DAYS)
          .build();

  private JavaUtils() {}

  public static long timeStringAsSec(String str) {
    return timeStringAs(str, TimeUnit.SECONDS);
  }

  public static long timeStringAs(String str, TimeUnit unit) {
    String lower = str.toLowerCase(Locale.ROOT).trim();

    try {
      Matcher matcher = Pattern.compile("(-?[0-9]+)([a-z]+)?").matcher(lower);
      if (!matcher.matches()) {
        throw new NumberFormatException("Failed to parse time string: " + str);
      }

      long val = Long.parseLong(matcher.group(1));
      String suffix = matcher.group(2);

      // Check for invalid suffixes
      if (suffix != null && !TIME_SUFFIXES.containsKey(suffix)) {
        throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
      }

      // If suffix is valid use that, otherwise none was provided and use the default passed
      return unit.convert(val, suffix != null ? TIME_SUFFIXES.get(suffix) : unit);
    } catch (NumberFormatException e) {
      String timeError =
          "Time must be specified as seconds (s), "
              + "milliseconds (ms), microseconds (us), minutes (m or min), hour (h), or day (d). "
              + "E.g. 50s, 100ms, or 250us.";

      throw new NumberFormatException(timeError + "\n" + e.getMessage());
    }
  }
}
