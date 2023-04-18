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

import org.apache.iceberg.util.DateTimeUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.functions.BoundFunction;
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.TimestampType;

/**
 * A Spark function implementation for the Iceberg year transform.
 *
 * <p>Example usage: {@code SELECT system.years('source_col')}.
 */
public class YearsFunction extends UnaryUnboundFunction {

  @Override
  protected BoundFunction doBind(DataType valueType) {
    if (valueType instanceof DateType) {
      return new DateToYearsFunction();
    } else if (valueType instanceof TimestampType) {
      return new TimestampToYearsFunction();
    } else {
      throw new UnsupportedOperationException(
          "Expected value to be date or timestamp: " + valueType.catalogString());
    }
  }

  @Override
  public String description() {
    return name()
        + "(col) - Call Iceberg's year transform\n"
        + "  col :: source column (must be date or timestamp)";
  }

  @Override
  public String name() {
    return "years";
  }

  private abstract static class BaseToYearsFunction implements ScalarFunction<Integer> {
    @Override
    public String name() {
      return "years";
    }

    @Override
    public DataType resultType() {
      return DataTypes.IntegerType;
    }
  }

  public static class DateToYearsFunction extends BaseToYearsFunction {
    // magic method used in codegen
    public static int invoke(int days) {
      return DateTimeUtil.daysToYears(days);
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {DataTypes.DateType};
    }

    @Override
    public String canonicalName() {
      return "iceberg.years(date)";
    }

    @Override
    public Integer produceResult(InternalRow input) {
      // return null for null input to match what Spark does in codegen
      return input.isNullAt(0) ? null : invoke(input.getInt(0));
    }
  }

  public static class TimestampToYearsFunction extends BaseToYearsFunction {
    // magic method used in codegen
    public static int invoke(long micros) {
      return DateTimeUtil.microsToYears(micros);
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {DataTypes.TimestampType};
    }

    @Override
    public String canonicalName() {
      return "iceberg.years(timestamp)";
    }

    @Override
    public Integer produceResult(InternalRow input) {
      // return null for null input to match what Spark does in codegen
      return input.isNullAt(0) ? null : invoke(input.getLong(0));
    }
  }
}
