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

package org.apache.iceberg.data;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestTableMigrationUtil {

  @Test
  public void testCanImportSchemaBasic() {
    // Same schema
    Schema srcSchema = new Schema(required(1, "id", Types.IntegerType.get()));
    Schema tgtSchema = new Schema(required(2, "id", Types.IntegerType.get()));
    Assert.assertTrue(TableMigrationUtil.canImportSchema(srcSchema, tgtSchema));

    // Nullability mismatch
    srcSchema = new Schema(optional(1, "id", Types.IntegerType.get()));
    tgtSchema = new Schema(required(1, "id", Types.IntegerType.get()));
    Assert.assertFalse(TableMigrationUtil.canImportSchema(srcSchema, tgtSchema));

    // Type mismatch
    srcSchema = new Schema(required(1, "id", Types.IntegerType.get()));
    tgtSchema = new Schema(required(1, "id", Types.StringType.get()));
    Assert.assertFalse(TableMigrationUtil.canImportSchema(srcSchema, tgtSchema));

    // part_col is optional
    tgtSchema = new Schema(
        required(1, "id", Types.IntegerType.get()),
        optional(2, "part_col", Types.IntegerType.get())
    );
    srcSchema = new Schema(required(1, "id", Types.IntegerType.get()));
    Assert.assertTrue(TableMigrationUtil.canImportSchema(srcSchema, tgtSchema));

    // part_col is required
    tgtSchema = new Schema(
        required(1, "id", Types.IntegerType.get()),
        required(2, "part_col", Types.IntegerType.get())
    );
    srcSchema = new Schema(required(1, "id", Types.IntegerType.get()));
    Assert.assertFalse(TableMigrationUtil.canImportSchema(srcSchema, tgtSchema));
  }

  @Test
  public void testValidateSchemaForTypePromotions() {
    Schema tgtSchema = new Schema(required(1, "id", Types.LongType.get()));
    Schema srcSchema = new Schema(required(1, "id", Types.IntegerType.get()));
    Assert.assertTrue(TableMigrationUtil.canImportSchema(srcSchema, tgtSchema));

    tgtSchema = new Schema(required(1, "id", Types.IntegerType.get()));
    srcSchema = new Schema(required(1, "id", Types.LongType.get()));
    Assert.assertFalse(TableMigrationUtil.canImportSchema(srcSchema, tgtSchema));
  }

  @Test
  public void testValidateSchemaForNestedSchema() {
    // Base case
    Schema tgtSchema = new Schema(
        required(1, "col1", Types.LongType.get()),
        optional(2, "col2", Types.StructType.of(
            required(3, "ncol1", Types.BooleanType.get())
        ))
    );
    Schema srcSchema = new Schema(
        required(1, "col1", Types.LongType.get())
    );
    Assert.assertTrue(TableMigrationUtil.canImportSchema(srcSchema, tgtSchema));

    // Nested field instead of Struct, with same name
    srcSchema = new Schema(
        required(1, "col1", Types.LongType.get()),
        optional(2, "col2", Types.IntegerType.get())
    );
    Assert.assertFalse(TableMigrationUtil.canImportSchema(srcSchema, tgtSchema));

    // Struct with nested field with different nullability
    srcSchema = new Schema(
        required(1, "col1", Types.LongType.get()),
        optional(2, "col2", Types.StructType.of(
            optional(3, "ncol1", Types.BooleanType.get())
        ))
    );
    Assert.assertFalse(TableMigrationUtil.canImportSchema(srcSchema, tgtSchema));

    // Struct with NestedField but different type
    srcSchema = new Schema(
        required(1, "col1", Types.LongType.get()),
        optional(2, "col2", Types.StructType.of(
            optional(3, "ncol1", Types.IntegerType.get())
        ))
    );
    Assert.assertFalse(TableMigrationUtil.canImportSchema(srcSchema, tgtSchema));
  }
}
