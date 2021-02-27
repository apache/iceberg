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


package org.apache.iceberg.types;

import org.apache.iceberg.Schema;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.required;


public class TestTypeUtil {
  @Test
  public void testReassignIdsDuplicateColumns() {
    Schema schema = new Schema(
        required(0, "a", Types.IntegerType.get()),
        required(1, "A", Types.IntegerType.get())
    );
    Schema sourceSchema = new Schema(
        required(1, "a", Types.IntegerType.get()),
        required(2, "A", Types.IntegerType.get())
    );
    final Schema actualSchema = TypeUtil.reassignIds(schema, sourceSchema);
    Assert.assertEquals(sourceSchema.asStruct(), actualSchema.asStruct());
  }

  @Test(expected = RuntimeException.class)
  public void testValidateSchemaViaIndexByName() {
    Types.NestedField nestedType = Types.NestedField
        .required(1, "a", Types.StructType.of(
            required(2, "b", Types.StructType.of(
                required(3, "c", Types.BooleanType.get())
            )),
            required(4, "b.c", Types.BooleanType.get())
            )
        );

    TypeUtil.indexByName(Types.StructType.of(nestedType));
  }
}

