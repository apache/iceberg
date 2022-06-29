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

package org.apache.iceberg.util;

import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Test;

import static org.junit.Assert.assertNotEquals;

public class TestStructLikeWrapper {
  @Test
  public void testEqualsWithDifferentTypes() {
    Types.NestedField intColumn = Types.NestedField.required(1, "id", Types.IntegerType.get());
    Types.NestedField longColumn = Types.NestedField.optional(2, "data", Types.LongType.get());
    Types.NestedField stringColumn = Types.NestedField.required(3, "name", Types.StringType.get());
    Schema schema = new Schema(intColumn, longColumn, stringColumn);

    StructLikeWrapper intAndLongType = StructLikeWrapper.forType(Types.StructType.of(intColumn, longColumn));
    StructLikeWrapper longAndStringType = StructLikeWrapper.forType(Types.StructType.of(longColumn, stringColumn));

    assertNotEquals(intAndLongType, longAndStringType);

    intAndLongType.set(new PartitionKey(PartitionSpec.builderFor(schema)
            .identity(intColumn.name()).identity(longColumn.name()).build(), schema));
    longAndStringType.set(new PartitionKey(PartitionSpec.builderFor(schema)
            .identity(longColumn.name()).identity(stringColumn.name()).build(), schema));

    assertNotEquals(intAndLongType, longAndStringType);
  }
}
