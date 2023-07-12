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
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkV2Filters;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkFunctions {

  private SparkFunctions() {}

  private static final Map<String, UnboundFunction> FUNCTIONS =
      ImmutableMap.of(
          "iceberg_version", new IcebergVersionFunction(),
          "years", new YearsFunction(),
          "months", new MonthsFunction(),
          "days", new DaysFunction(),
          "hours", new HoursFunction(),
          "bucket", new BucketFunction(),
          "truncate", new TruncateFunction());

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

  public static ScalarFunction<Object> buildFromClass(
      Class<?> functionClass, DataType[] inputTypes) {
    String clasCanonicalName = functionClass.getCanonicalName();
    if (!clasCanonicalName.startsWith(SparkFunctions.class.getPackage().getName())) {
      return null;
    }

    try {
      Class<?> declaringClass = functionClass.getDeclaringClass();
      DynConstructors.Ctor<?> ctor =
          DynConstructors.builder().impl(declaringClass.getName()).build();

      UnboundFunction unboundFunction = (UnboundFunction) ctor.newInstance();

      // Build input data type
      StructField[] fields = new StructField[inputTypes.length];
      for (int i = 0; i < inputTypes.length; i++) {
        // system function binding only cares about input datatype
        fields[i] = DataTypes.createStructField("dummy_" + i, inputTypes[i], false);
      }
      StructType inputType = DataTypes.createStructType(fields);

      // Binding with built data type
      ScalarFunction<Object> function = (ScalarFunction<Object>) unboundFunction.bind(inputType);

      if (SparkV2Filters.SUPPORTED_FUNCTIONS.contains(function.name())) {
        return function;
      }

      return null;
    } catch (Exception e) {
      return null;
    }
  }
}
