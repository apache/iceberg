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

import org.apache.spark.sql.connector.catalog.functions.ScalarFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestSparkFunctions {

  @Test
  public void testBuildYearsFunctionFromClass() {
    YearsFunction.DateToYearsFunction dateToYearsFunc = new YearsFunction.DateToYearsFunction();
    checkBuildFunc(dateToYearsFunc);

    YearsFunction.TimestampToYearsFunction tsToYearsFunc =
        new YearsFunction.TimestampToYearsFunction();
    checkBuildFunc(tsToYearsFunc);

    YearsFunction.TimestampNtzToYearsFunction tsNtzToYearsFunc =
        new YearsFunction.TimestampNtzToYearsFunction();
    checkBuildFunc(tsNtzToYearsFunc);
  }

  @Test
  public void testBuildMonthsFunctionFromClass() {
    MonthsFunction.DateToMonthsFunction dateToMonthsFunc =
        new MonthsFunction.DateToMonthsFunction();
    checkBuildFunc(dateToMonthsFunc);

    MonthsFunction.TimestampToMonthsFunction tsToMonthsFunc =
        new MonthsFunction.TimestampToMonthsFunction();
    checkBuildFunc(tsToMonthsFunc);

    MonthsFunction.TimestampNtzToMonthsFunction tsNtzToMonthsFunc =
        new MonthsFunction.TimestampNtzToMonthsFunction();
    checkBuildFunc(tsNtzToMonthsFunc);
  }

  @Test
  public void testBuildDaysFunctionFromClass() {
    DaysFunction.DateToDaysFunction dateToDaysFunc = new DaysFunction.DateToDaysFunction();
    checkBuildFunc(dateToDaysFunc);

    DaysFunction.TimestampToDaysFunction tsToDaysFunc = new DaysFunction.TimestampToDaysFunction();
    checkBuildFunc(tsToDaysFunc);

    DaysFunction.TimestampNtzToDaysFunction tsNtzToDaysFunc =
        new DaysFunction.TimestampNtzToDaysFunction();
    checkBuildFunc(tsNtzToDaysFunc);
  }

  @Test
  public void testBuildHoursFunctionFromClass() {
    HoursFunction.TimestampToHoursFunction tsToHoursFunc =
        new HoursFunction.TimestampToHoursFunction();
    checkBuildFunc(tsToHoursFunc);

    HoursFunction.TimestampNtzToHoursFunction tsNtzToHoursFunc =
        new HoursFunction.TimestampNtzToHoursFunction();
    checkBuildFunc(tsNtzToHoursFunc);
  }

  @Test
  public void testBuildBucketFunctionFromClass() {
    BucketFunction.BucketInt bucketDateFunc = new BucketFunction.BucketInt(DataTypes.DateType);
    checkBuildFunc(bucketDateFunc);

    BucketFunction.BucketInt bucketIntFunc = new BucketFunction.BucketInt(DataTypes.IntegerType);
    checkBuildFunc(bucketIntFunc);

    BucketFunction.BucketLong bucketLongFunc = new BucketFunction.BucketLong(DataTypes.LongType);
    checkBuildFunc(bucketLongFunc);

    BucketFunction.BucketLong bucketTsFunc = new BucketFunction.BucketLong(DataTypes.TimestampType);
    checkBuildFunc(bucketTsFunc);

    BucketFunction.BucketLong bucketTsNtzFunc =
        new BucketFunction.BucketLong(DataTypes.TimestampNTZType);
    checkBuildFunc(bucketTsNtzFunc);

    BucketFunction.BucketDecimal bucketDecimalFunc =
        new BucketFunction.BucketDecimal(new DecimalType());
    checkBuildFunc(bucketDecimalFunc);

    BucketFunction.BucketString bucketStringFunc = new BucketFunction.BucketString();
    checkBuildFunc(bucketStringFunc);

    BucketFunction.BucketBinary bucketBinary = new BucketFunction.BucketBinary();
    checkBuildFunc(bucketBinary);
  }

  @Test
  public void testBuildTruncateFunctionFromClass() {
    TruncateFunction.TruncateTinyInt truncateTinyIntFunc = new TruncateFunction.TruncateTinyInt();
    checkBuildFunc(truncateTinyIntFunc);

    TruncateFunction.TruncateSmallInt truncateSmallIntFunc =
        new TruncateFunction.TruncateSmallInt();
    checkBuildFunc(truncateSmallIntFunc);

    TruncateFunction.TruncateInt truncateIntFunc = new TruncateFunction.TruncateInt();
    checkBuildFunc(truncateIntFunc);

    TruncateFunction.TruncateBigInt truncateBigIntFunc = new TruncateFunction.TruncateBigInt();
    checkBuildFunc(truncateBigIntFunc);

    TruncateFunction.TruncateDecimal truncateDecimalFunc =
        new TruncateFunction.TruncateDecimal(10, 9);
    checkBuildFunc(truncateDecimalFunc);

    TruncateFunction.TruncateString truncateStringFunc = new TruncateFunction.TruncateString();
    checkBuildFunc(truncateStringFunc);

    TruncateFunction.TruncateBinary truncateBinaryFunc = new TruncateFunction.TruncateBinary();
    checkBuildFunc(truncateBinaryFunc);
  }

  @Test
  public void testNotSupportedFunc() {
    IcebergVersionFunction.IcebergVersionFunctionImpl versionFunction =
        new IcebergVersionFunction.IcebergVersionFunctionImpl();
    ScalarFunction<Object> actual =
        SparkFunctions.buildFromClass(versionFunction.getClass(), versionFunction.inputTypes());
    Assertions.assertThat(actual).isNull();
  }

  private void checkBuildFunc(ScalarFunction<?> expected) {
    ScalarFunction<Object> actual =
        SparkFunctions.buildFromClass(expected.getClass(), expected.inputTypes());

    Assertions.assertThat(actual).isNotNull();
    Assertions.assertThat(actual.name()).isEqualTo(expected.name());
    Assertions.assertThat(actual.canonicalName()).isEqualTo(expected.canonicalName());
  }
}
