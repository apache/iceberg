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
package org.apache.iceberg.spark.functions;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;

public class SparkFunctions {

  private SparkFunctions() {}

  private static final UnboundFunction VERSION_FUNCTION = new IcebergVersionFunction();
  private static final UnboundFunction YEAR_FUNCTION = new YearsFunction();
  private static final UnboundFunction YEARS_FUNCTION = new YearsFunction(false);
  private static final UnboundFunction MONTH_FUNCTION = new MonthsFunction();
  private static final UnboundFunction MONTHS_FUNCTION = new MonthsFunction(false);
  private static final UnboundFunction DAY_FUNCTION = new DaysFunction();
  private static final UnboundFunction DAYS_FUNCTION = new DaysFunction(false);
  private static final UnboundFunction HOUR_FUNCTION = new HoursFunction();
  private static final UnboundFunction HOURS_FUNCTION = new HoursFunction(false);
  private static final UnboundFunction BUCKET_FUNCTION = new BucketFunction();
  private static final UnboundFunction TRUNCATE_FUNCTION = new TruncateFunction();

  private static final Map<String, UnboundFunction> FUNCTIONS =
      new ImmutableMap.Builder<String, UnboundFunction>()
          .put("iceberg_version", VERSION_FUNCTION)
          .put("year", YEAR_FUNCTION)
          .put("years", YEARS_FUNCTION)
          .put("month", MONTH_FUNCTION)
          .put("months", MONTHS_FUNCTION)
          .put("day", DAY_FUNCTION)
          .put("days", DAYS_FUNCTION)
          .put("hour", HOUR_FUNCTION)
          .put("hours", HOURS_FUNCTION)
          .put("bucket", BUCKET_FUNCTION)
          .put("truncate", TRUNCATE_FUNCTION)
          .build();

  private static final Map<Class<?>, UnboundFunction> CLASS_TO_FUNCTIONS =
      ImmutableMap.of(
          YearsFunction.class, YEAR_FUNCTION,
          MonthsFunction.class, MONTH_FUNCTION,
          DaysFunction.class, DAY_FUNCTION,
          HoursFunction.class, HOUR_FUNCTION,
          BucketFunction.class, BUCKET_FUNCTION,
          TruncateFunction.class, TRUNCATE_FUNCTION);

  private static final List<String> FUNCTION_NAMES = ImmutableList.copyOf(FUNCTIONS.keySet());

  // Functions that are added to all Iceberg catalogs should be accessed with the `system`
  // namespace. They can also be accessed with no namespace at all if qualified with the
  // catalog name, e.g. my_hadoop_catalog.iceberg_version().
  // As namespace resolution is handled by those rules in BaseCatalog, a list of names
  // alone is returned.
  public static List<String> list() {
    return FUNCTION_NAMES;
  }

  public static UnboundFunction load(String name) {
    // function resolution is case-insensitive to match the existing Spark behavior for functions
    return FUNCTIONS.get(name.toLowerCase(Locale.ROOT));
  }

  public static UnboundFunction loadFunctionByClass(Class<?> functionClass) {
    Class<?> declaringClass = functionClass.getDeclaringClass();
    if (declaringClass == null) {
      return null;
    }

    return CLASS_TO_FUNCTIONS.get(declaringClass);
  }
}
