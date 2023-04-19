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

import java.util.Set;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestStructLikeSet {
  private static final Types.StructType STRUCT_TYPE =
      Types.StructType.of(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.LongType.get()));

  @Test
  public void testNullElements() {
    Set<StructLike> set = StructLikeSet.create(STRUCT_TYPE);
    Assert.assertFalse(set.contains(null));

    set.add(null);
    Assert.assertTrue(set.contains(null));

    boolean added = set.add(null);
    Assert.assertFalse(added);

    boolean removed = set.remove(null);
    Assert.assertTrue(removed);
    Assert.assertTrue(set.isEmpty());
  }

  @Test
  public void testElementsWithNulls() {
    Record recordTemplate = GenericRecord.create(STRUCT_TYPE);
    Record record1 = recordTemplate.copy("id", 1, "data", null);
    Record record2 = recordTemplate.copy("id", 2, "data", null);

    Set<StructLike> set = StructLikeSet.create(STRUCT_TYPE);
    set.add(record1);
    set.add(record2);

    Assert.assertTrue(set.contains(record1));
    Assert.assertTrue(set.contains(record2));

    Record record3 = record1.copy();
    Assert.assertTrue(set.contains(record3));

    boolean removed = set.remove(record3);
    Assert.assertTrue(removed);
  }
}
