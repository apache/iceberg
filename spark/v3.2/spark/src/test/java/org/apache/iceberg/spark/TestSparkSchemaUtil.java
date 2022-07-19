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

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

public class TestSparkSchemaUtil {
  private static final Schema TEST_SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));

  private static final Schema TEST_SCHEMA_WITH_METADATA_COLS =
      new Schema(
          optional(1, "id", Types.IntegerType.get()),
          optional(2, "data", Types.StringType.get()),
          MetadataColumns.FILE_PATH,
          MetadataColumns.ROW_POSITION);

  @Test
  public void testEstimateSizeMaxValue() throws IOException {
    Assert.assertEquals(
        "estimateSize returns Long max value",
        Long.MAX_VALUE,
        SparkSchemaUtil.estimateSize(null, Long.MAX_VALUE));
  }

  @Test
  public void testEstimateSizeWithOverflow() throws IOException {
    long tableSize =
        SparkSchemaUtil.estimateSize(SparkSchemaUtil.convert(TEST_SCHEMA), Long.MAX_VALUE - 1);
    Assert.assertEquals("estimateSize handles overflow", Long.MAX_VALUE, tableSize);
  }

  @Test
  public void testEstimateSize() throws IOException {
    long tableSize = SparkSchemaUtil.estimateSize(SparkSchemaUtil.convert(TEST_SCHEMA), 1);
    Assert.assertEquals("estimateSize matches with expected approximation", 24, tableSize);
  }

  @Test
  public void testSchemaConversionWithMetaDataColumnSchema() {
    StructType structType = SparkSchemaUtil.convert(TEST_SCHEMA_WITH_METADATA_COLS);
    List<AttributeReference> attrRefs =
        scala.collection.JavaConverters.seqAsJavaList(structType.toAttributes());
    for (AttributeReference attrRef : attrRefs) {
      if (MetadataColumns.isMetadataColumn(attrRef.name())) {
        Assert.assertTrue(
            "metadata columns should have __metadata_col in attribute metadata",
            attrRef.metadata().contains(TypeToSparkType.METADATA_COL_ATTR_KEY)
                && attrRef.metadata().getBoolean(TypeToSparkType.METADATA_COL_ATTR_KEY));
      } else {
        Assert.assertFalse(
            "non metadata columns should not have __metadata_col in attribute metadata",
            attrRef.metadata().contains(TypeToSparkType.METADATA_COL_ATTR_KEY));
      }
    }
  }
}
