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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;

public class DataTestHelpers {
  private DataTestHelpers() {}

  public static void assertEquals(Types.StructType struct, Record expected, Record actual) {
    List<Types.NestedField> fields = struct.fields();
    for (int i = 0; i < fields.size(); i += 1) {
      Type fieldType = fields.get(i).type();

      Object expectedValue = expected.get(i);
      Object actualValue = actual.get(i);

      assertEquals(fieldType, expectedValue, actualValue);
    }
  }

  public static void assertEquals(Types.ListType list, List<?> expected, List<?> actual) {
    Type elementType = list.elementType();

    Assert.assertEquals("List size should match", expected.size(), actual.size());

    for (int i = 0; i < expected.size(); i += 1) {
      Object expectedValue = expected.get(i);
      Object actualValue = actual.get(i);

      assertEquals(elementType, expectedValue, actualValue);
    }
  }

  public static void assertEquals(Types.MapType map, Map<?, ?> expected, Map<?, ?> actual) {
    Type valueType = map.valueType();

    Assert.assertEquals("Map size should match", expected.size(), actual.size());

    for (Object expectedKey : expected.keySet()) {
      Object expectedValue = expected.get(expectedKey);
      Object actualValue = actual.get(expectedKey);

      assertEquals(valueType, expectedValue, actualValue);
    }
  }

  private static void assertEquals(Type type, Object expected, Object actual) {
    if (expected == null && actual == null) {
      return;
    }

    switch (type.typeId()) {
      case BOOLEAN:
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case UUID:
      case BINARY:
      case DECIMAL:
        Assert.assertEquals("Primitive value should be equal to expected for type " + type, expected, actual);
        break;
      case FIXED:
        Assert.assertTrue("Expected should be a byte[]", expected instanceof byte[]);
        Assert.assertTrue("Actual should be a byte[]", actual instanceof byte[]);
        Assert.assertArrayEquals("Array contents should be equal",
            (byte[]) expected, (byte[]) actual);
        break;
      case STRUCT:
        Assert.assertTrue("Expected should be a Record", expected instanceof Record);
        Assert.assertTrue("Actual should be a Record", actual instanceof Record);
        assertEquals(type.asStructType(), (Record) expected, (Record) actual);
        break;
      case LIST:
        Assert.assertTrue("Expected should be a List", expected instanceof List);
        Assert.assertTrue("Actual should be a List", actual instanceof List);
        assertEquals(type.asListType(), (List) expected, (List) actual);
        break;
      case MAP:
        Assert.assertTrue("Expected should be a Map", expected instanceof Map);
        Assert.assertTrue("Actual should be a Map", actual instanceof Map);
        assertEquals(type.asMapType(), (Map<?, ?>) expected, (Map<?, ?>) actual);
        break;
      default:
        throw new IllegalArgumentException("Not a supported type: " + type);
    }
  }
}
