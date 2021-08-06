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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
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

  @Test
  public void testReassignIdsWithIdentifier() {
    Schema schema = new Schema(
        Lists.newArrayList(
            required(0, "a", Types.IntegerType.get()),
            required(1, "A", Types.IntegerType.get())),
        Sets.newHashSet(0)
    );
    Schema sourceSchema = new Schema(
        Lists.newArrayList(
            required(1, "a", Types.IntegerType.get()),
            required(2, "A", Types.IntegerType.get())),
        Sets.newHashSet(1)
    );
    final Schema actualSchema = TypeUtil.reassignIds(schema, sourceSchema);
    Assert.assertEquals(sourceSchema.asStruct(), actualSchema.asStruct());
    Assert.assertEquals("identifier field ID should change based on source schema",
        sourceSchema.identifierFieldIds(), actualSchema.identifierFieldIds());
  }

  @Test
  public void testAssignIncreasingFreshIdWithIdentifier() {
    Schema schema = new Schema(
        Lists.newArrayList(
            required(10, "a", Types.IntegerType.get()),
            required(11, "A", Types.IntegerType.get())),
        Sets.newHashSet(10)
    );
    Schema expectedSchema = new Schema(
        Lists.newArrayList(
            required(1, "a", Types.IntegerType.get()),
            required(2, "A", Types.IntegerType.get())),
        Sets.newHashSet(1)
    );
    final Schema actualSchema = TypeUtil.assignIncreasingFreshIds(schema);
    Assert.assertEquals(expectedSchema.asStruct(), actualSchema.asStruct());
    Assert.assertEquals("identifier field ID should change based on source schema",
        expectedSchema.identifierFieldIds(), actualSchema.identifierFieldIds());
  }

  @Test
  public void testAssignIncreasingFreshIdNewIdentifier() {
    Schema schema = new Schema(
        Lists.newArrayList(
            required(10, "a", Types.IntegerType.get()),
            required(11, "A", Types.IntegerType.get())),
        Sets.newHashSet(10)
    );
    Schema sourceSchema = new Schema(
        Lists.newArrayList(
            required(1, "a", Types.IntegerType.get()),
            required(2, "A", Types.IntegerType.get()))
    );
    final Schema actualSchema = TypeUtil.reassignIds(schema, sourceSchema);
    Assert.assertEquals(sourceSchema.asStruct(), actualSchema.asStruct());
    Assert.assertEquals("source schema missing identifier should not impact refreshing new identifier",
        Sets.newHashSet(sourceSchema.findField("a").fieldId()), actualSchema.identifierFieldIds());
  }

  @Test
  public void testProject() {
    Schema schema = new Schema(
        Lists.newArrayList(
            required(10, "a", Types.IntegerType.get()),
            required(11, "A", Types.IntegerType.get()),
            required(12, "someStruct", Types.StructType.of(
                required(13, "b", Types.IntegerType.get()),
                required(14, "B", Types.IntegerType.get()),
                required(15, "anotherStruct", Types.StructType.of(
                    required(16, "c", Types.IntegerType.get()),
                    required(17, "C", Types.IntegerType.get()))
                )))));

    Schema expectedTop = new Schema(
        Lists.newArrayList(
            required(11, "A", Types.IntegerType.get())));

    Schema actualTop = TypeUtil.project(schema, Sets.newHashSet(11));
    Assert.assertEquals(expectedTop.asStruct(), actualTop.asStruct());

    Schema expectedDepthOne = new Schema(
        Lists.newArrayList(
            required(10, "a", Types.IntegerType.get()),
            required(12, "someStruct", Types.StructType.of(
                required(13, "b", Types.IntegerType.get())))));

    Schema actualDepthOne = TypeUtil.project(schema, Sets.newHashSet(10, 12, 13));
    Assert.assertEquals(expectedDepthOne.asStruct(), actualDepthOne.asStruct());

    Schema expectedDepthTwo = new Schema(
        Lists.newArrayList(
            required(11, "A", Types.IntegerType.get()),
            required(12, "someStruct", Types.StructType.of(
                required(15, "anotherStruct", Types.StructType.of(
                    required(17, "C", Types.IntegerType.get()))
                )))));

    Schema actualDepthTwo = TypeUtil.project(schema, Sets.newHashSet(11, 12, 15, 17));
    Assert.assertEquals(expectedDepthTwo.asStruct(), actualDepthTwo.asStruct());
  }

  @Test
  public void testProjectEmpty() {
    Schema schema = new Schema(
        Lists.newArrayList(
            required(10, "a", Types.IntegerType.get()),
            required(11, "A", Types.IntegerType.get()),
            required(12, "someStruct", Types.StructType.of(
                required(13, "b", Types.IntegerType.get()),
                required(14, "B", Types.IntegerType.get()),
                required(15, "anotherStruct", Types.StructType.of(
                    required(16, "c", Types.IntegerType.get()),
                    required(17, "C", Types.IntegerType.get()))
            )))));

    Schema expectedDepthOne = new Schema(
        Lists.newArrayList(
            required(12, "someStruct", Types.StructType.of())));

    Schema actualDepthOne = TypeUtil.project(schema, Sets.newHashSet(12));
    Assert.assertEquals(expectedDepthOne.asStruct(), actualDepthOne.asStruct());

    Schema expectedDepthTwo = new Schema(
        Lists.newArrayList(
            required(12, "someStruct", Types.StructType.of(
                required(15, "anotherStruct", Types.StructType.of())))));

    Schema actualDepthTwo = TypeUtil.project(schema, Sets.newHashSet(12, 15));
    Assert.assertEquals(expectedDepthTwo.asStruct(), actualDepthTwo.asStruct());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testReassignIdsIllegalArgumentException() {
    Schema schema = new Schema(
        required(1, "a", Types.IntegerType.get()),
        required(2, "b", Types.IntegerType.get())
    );
    Schema sourceSchema = new Schema(
        required(1, "a", Types.IntegerType.get())
    );
    TypeUtil.reassignIds(schema, sourceSchema);
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
