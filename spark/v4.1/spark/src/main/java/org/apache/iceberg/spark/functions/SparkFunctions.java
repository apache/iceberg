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

  private static final Map<String, UnboundFunction> FUNCTIONS =
      ImmutableMap.<String, UnboundFunction>builder()
          .put("iceberg_version", new IcebergVersionFunction())
          .put("years", new YearsFunction())
          .put("months", new MonthsFunction())
          .put("days", new DaysFunction())
          .put("hours", new HoursFunction())
          .put("bucket", new BucketFunction())
          .put("truncate", new TruncateFunction())
          .put("iceberg_mask_alphanum", new MaskAlphanumFunction())
          .build();

  private static final Map<Class<?>, UnboundFunction> CLASS_TO_FUNCTIONS =
      ImmutableMap.<Class<?>, UnboundFunction>builder()
          .put(YearsFunction.class, new YearsFunction())
          .put(MonthsFunction.class, new MonthsFunction())
          .put(DaysFunction.class, new DaysFunction())
          .put(HoursFunction.class, new HoursFunction())
          .put(BucketFunction.class, new BucketFunction())
          .put(TruncateFunction.class, new TruncateFunction())
          .put(MaskAlphanumFunction.class, new MaskAlphanumFunction())
          .build();

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
