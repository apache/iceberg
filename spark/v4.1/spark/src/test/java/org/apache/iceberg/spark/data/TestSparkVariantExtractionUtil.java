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
package org.apache.iceberg.spark.data;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.VariantType$;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

class TestSparkVariantExtractionUtil {

  private static final DataType[] SUPPORTED_TARGET_TYPES =
      new DataType[] {
        DataTypes.StringType,
        DataTypes.IntegerType,
        DataTypes.LongType,
        DataTypes.ByteType,
        DataTypes.ShortType,
        DataTypes.BooleanType,
        DataTypes.FloatType,
        DataTypes.DoubleType,
        DataTypes.createDecimalType(9, 2),
        DataTypes.DateType,
        DataTypes.TimestampType,
        DataTypes.TimestampNTZType,
        DataTypes.BinaryType
      };

  private static final DataType[] UNSUPPORTED_TARGET_TYPES =
      new DataType[] {
        VariantType$.MODULE$,
        new StructType(
            new StructField[] {DataTypes.createStructField("a", DataTypes.IntegerType, true)}),
        DataTypes.createArrayType(DataTypes.IntegerType),
        DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType),
        DataTypes.createCharType(4),
        DataTypes.createVarcharType(4)
      };

  @ParameterizedTest
  @FieldSource("SUPPORTED_TARGET_TYPES")
  void isSupportedPushdownTargetTypeAcceptsSupportedTypes(DataType targetType) {
    assertThat(SparkVariantExtractionUtil.isSupportedPushdownTargetType(targetType)).isTrue();
  }

  @ParameterizedTest
  @FieldSource("UNSUPPORTED_TARGET_TYPES")
  void isSupportedPushdownTargetTypeRejectsUnsupportedTypes(DataType targetType) {
    assertThat(SparkVariantExtractionUtil.isSupportedPushdownTargetType(targetType)).isFalse();
  }

  @Test
  void isSupportedExtractionPathAcceptsAllValidPaths() {
    assertThat(SparkVariantExtractionUtil.isSupportedExtractionPath("$.size")).isTrue();
    assertThat(SparkVariantExtractionUtil.isSupportedExtractionPath("$.pull_request.user.login"))
        .isTrue();
    assertThat(SparkVariantExtractionUtil.isSupportedExtractionPath("$.commits[0].author.name"))
        .isTrue();
    assertThat(
            SparkVariantExtractionUtil.isSupportedExtractionPath("$['issue']['labels'][0]['name']"))
        .isTrue();
  }

  @Test
  void failOnErrorReadsMetadataFlag() {
    assertThat(SparkVariantExtractionUtil.failOnError(extractionField(true))).isTrue();
    assertThat(SparkVariantExtractionUtil.failOnError(extractionField(false))).isFalse();
  }

  @Test
  void failOnErrorDefaultsFalseWhenAbsent() {
    Metadata variantMetadata = new MetadataBuilder().putString("path", "$.size").build();
    Metadata metadata =
        new MetadataBuilder()
            .putMetadata(SparkVariantExtractionUtil.VARIANT_METADATA_KEY, variantMetadata)
            .build();
    StructField field = DataTypes.createStructField("0", DataTypes.LongType, true, metadata);
    assertThat(SparkVariantExtractionUtil.failOnError(field)).isFalse();
  }

  private static StructField extractionField(boolean failOnError) {
    Metadata variantMetadata =
        new MetadataBuilder()
            .putString("path", "$.size")
            .putBoolean("failOnError", failOnError)
            .build();
    Metadata metadata =
        new MetadataBuilder()
            .putMetadata(SparkVariantExtractionUtil.VARIANT_METADATA_KEY, variantMetadata)
            .build();
    return DataTypes.createStructField("0", DataTypes.LongType, true, metadata);
  }
}
