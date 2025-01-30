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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;

/**
 * A Spark function implementation for the Iceberg month transform.
 *
 * <p>Example usage: {@code SELECT system.month('source_col')}.
 *
 * <p>Alternate form: {@code SELECT system.months('source_col')}.
 */
public class MonthsFunction extends UnaryUnboundFunction {

  private boolean singular;

  public MonthsFunction() {
    this(true);
  }

  MonthsFunction(boolean singular) {
    this.singular = singular;
  }

  @Override
  protected BoundFunction doBind(DataType valueType) {
    if (valueType instanceof DateType) {
      return new DateToMonthsFunction(singular);
    } else if (valueType instanceof TimestampType) {
      return new TimestampToMonthsFunction(singular);
    } else if (valueType instanceof TimestampNTZType) {
      return new TimestampNtzToMonthsFunction(singular);
    } else {
      throw new UnsupportedOperationException(
          "Expected value to be date or timestamp: " + valueType.catalogString());
    }
  }

  @Override
  public String description() {
    return name()
        + "(col) - Call Iceberg's month transform\n"
        + "  col :: source column (must be date or timestamp)";
  }

  @Override
  public String name() {
    return singular ? "month" : "months";
  }

  private abstract static class BaseToMonthsFunction extends BaseScalarFunction<Integer> {
    private boolean singular;

    protected BaseToMonthsFunction(boolean singular) {
      this.singular = singular;
    }

    @Override
    public String name() {
      return singular ? "month" : "months";
    }

    @Override
    public DataType resultType() {
      return DataTypes.IntegerType;
    }
  }

  public static class DateToMonthsFunction extends BaseToMonthsFunction {
    public DateToMonthsFunction() {
      this(true);
    }

    DateToMonthsFunction(boolean singular) {
      super(singular);
    }

    // magic method used in codegen
    public static int invoke(int days) {
      return DateTimeUtil.daysToMonths(days);
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {DataTypes.DateType};
    }

    @Override
    public String canonicalName() {
      return "iceberg." + name() + "months(date)";
    }

    @Override
    public Integer produceResult(InternalRow input) {
      // return null for null input to match what Spark does in codegen
      return input.isNullAt(0) ? null : invoke(input.getInt(0));
    }
  }

  public static class TimestampToMonthsFunction extends BaseToMonthsFunction {
    public TimestampToMonthsFunction() {
      this(true);
    }

    TimestampToMonthsFunction(boolean singular) {
      super(singular);
    }

    // magic method used in codegen
    public static int invoke(long micros) {
      return DateTimeUtil.microsToMonths(micros);
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {DataTypes.TimestampType};
    }

    @Override
    public String canonicalName() {
      return "iceberg." + name() + "(timestamp)";
    }

    @Override
    public Integer produceResult(InternalRow input) {
      // return null for null input to match what Spark does in codegen
      return input.isNullAt(0) ? null : invoke(input.getLong(0));
    }
  }

  public static class TimestampNtzToMonthsFunction extends BaseToMonthsFunction {
    public TimestampNtzToMonthsFunction() {
      this(true);
    }

    TimestampNtzToMonthsFunction(boolean singular) {
      super(singular);
    }

    // magic method used in codegen
    public static int invoke(long micros) {
      return DateTimeUtil.microsToMonths(micros);
    }

    @Override
    public DataType[] inputTypes() {
      return new DataType[] {DataTypes.TimestampNTZType};
    }

    @Override
    public String canonicalName() {
      return "iceberg." + name() + "(timestamp_ntz)";
    }

    @Override
    public Integer produceResult(InternalRow input) {
      // return null for null input to match what Spark does in codegen
      return input.isNullAt(0) ? null : invoke(input.getLong(0));
    }
  }
}
