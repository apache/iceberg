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

import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestTableMigrationUtil {

  @Test
  public void testCanImportSchemaBasic() {
    // Same schema
    Schema srcSchema = new Schema(required(1, "id", Types.IntegerType.get()));
    Schema tgtSchema = new Schema(required(2, "id", Types.IntegerType.get()));
    TypeUtil.canImportSchema(srcSchema, tgtSchema);

    // Nullability mismatch
    Schema srcSchema1 = new Schema(optional(1, "id", Types.IntegerType.get()));
    Schema tgtSchema1 = new Schema(required(1, "id", Types.IntegerType.get()));
    AssertHelpers.assertThrows("Should throw validation exception on nullability mismatch",
        ValidationException.class, "schema not compatible",
        () -> TypeUtil.canImportSchema(srcSchema1, tgtSchema1));

    // Type mismatch
    Schema srcSchema2 = new Schema(required(1, "id", Types.IntegerType.get()));
    Schema tgtSchema2 = new Schema(required(1, "id", Types.StringType.get()));
    AssertHelpers.assertThrows("Should throw validation exception on type mismatch",
        ValidationException.class, "schema not compatible",
        () -> TypeUtil.canImportSchema(srcSchema2, tgtSchema2));

    // part_col is optional
    Schema tgtSchema3 = new Schema(
        required(1, "id", Types.IntegerType.get()),
        optional(2, "part_col", Types.IntegerType.get())
    );
    Schema srcSchema3 = new Schema(required(1, "id", Types.IntegerType.get()));
    TypeUtil.canImportSchema(srcSchema3, tgtSchema3);
  }

  @Test
  public void testValidateSchemaForTypePromotions() {
    Schema tgtSchema1 = new Schema(required(1, "id", Types.LongType.get()));
    Schema srcSchema1 = new Schema(required(1, "id", Types.IntegerType.get()));
    TypeUtil.canImportSchema(srcSchema1, tgtSchema1);

    Schema tgtSchema2 = new Schema(required(1, "id", Types.IntegerType.get()));
    Schema srcSchema2 = new Schema(required(1, "id", Types.LongType.get()));
    AssertHelpers.assertThrows("Should throw validation exception on incompatible type promotions",
        ValidationException.class, "schema not compatible",
        () -> TypeUtil.canImportSchema(srcSchema2, tgtSchema2));
  }

  @Test
  public void testValidateSchemaForNestedSchema() {
    // Base case
    Schema tgtSchema1 = new Schema(
        required(1, "col1", Types.LongType.get()),
        optional(2, "col2", Types.StructType.of(
            required(3, "ncol1", Types.BooleanType.get())
        ))
    );
    Schema srcSchema1 = new Schema(
        required(1, "col1", Types.LongType.get())
    );
    TypeUtil.canImportSchema(srcSchema1, tgtSchema1);

    // Nested field instead of Struct, with same name
    Schema srcSchema2 = new Schema(
        required(1, "col1", Types.LongType.get()),
        optional(2, "col2", Types.IntegerType.get())
    );
    AssertHelpers.assertThrows("Should throw validation exception",
        ValidationException.class, "schema not compatible",
        () -> TypeUtil.canImportSchema(srcSchema2, tgtSchema1));


    // Struct with nested field with different nullability
    Schema srcSchema3 = new Schema(
        required(1, "col1", Types.LongType.get()),
        optional(2, "col2", Types.StructType.of(
            optional(3, "ncol1", Types.BooleanType.get())
        ))
    );
    AssertHelpers.assertThrows("Should throw validation exception on nullability incompatible nested fields",
        ValidationException.class, "schema not compatible",
        () -> TypeUtil.canImportSchema(srcSchema3, tgtSchema1));

    // Struct with NestedField but different type
    Schema srcSchema4 = new Schema(
        required(1, "col1", Types.LongType.get()),
        optional(2, "col2", Types.StructType.of(
            optional(3, "ncol1", Types.IntegerType.get())
        ))
    );
    AssertHelpers.assertThrows("Should throw validation exception on type incompatible nested fields",
        ValidationException.class, "schema not compatible",
        () -> TypeUtil.canImportSchema(srcSchema4, tgtSchema1));
  }
}
