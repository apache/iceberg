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
package org.apache.iceberg.avro;

import static org.apache.iceberg.avro.AvroSchemaUtil.toOption;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

class AvroTestHelpers {

  private AvroTestHelpers() {}

  static Schema.Field optionalField(int id, String name, Schema schema) {
    return addId(id, new Schema.Field(name, toOption(schema), null, JsonProperties.NULL_VALUE));
  }

  static Schema.Field requiredField(int id, String name, Schema schema) {
    return addId(id, new Schema.Field(name, schema, null, null));
  }

  static Schema record(String name, Schema.Field... fields) {
    return Schema.createRecord(name, null, null, false, Arrays.asList(fields));
  }

  static Schema.Field addId(int id, Schema.Field field) {
    field.addProp(AvroSchemaUtil.FIELD_ID_PROP, id);
    return field;
  }

  static Schema addElementId(int id, Schema schema) {
    schema.addProp(AvroSchemaUtil.ELEMENT_ID_PROP, id);
    return schema;
  }

  static Schema addKeyId(int id, Schema schema) {
    schema.addProp(AvroSchemaUtil.KEY_ID_PROP, id);
    return schema;
  }

  static Schema addValueId(int id, Schema schema) {
    schema.addProp(AvroSchemaUtil.VALUE_ID_PROP, id);
    return schema;
  }

  static void assertEquals(Types.StructType struct, Record expected, Record actual) {
    List<Types.NestedField> fields = struct.fields();
    for (int i = 0; i < fields.size(); i += 1) {
      Type fieldType = fields.get(i).type();

      Object expectedValue = expected.get(i);
      Object actualValue = actual.get(i);

      assertEquals(fieldType, expectedValue, actualValue);
    }
  }

  static void assertEquals(Types.ListType list, List<?> expected, List<?> actual) {
    Type elementType = list.elementType();

    assertThat(actual).as("List size should match").hasSameSizeAs(expected);

    for (int i = 0; i < expected.size(); i += 1) {
      Object expectedValue = expected.get(i);
      Object actualValue = actual.get(i);

      assertEquals(elementType, expectedValue, actualValue);
    }
  }

  static void assertEquals(Types.MapType map, Map<?, ?> expected, Map<?, ?> actual) {
    Type valueType = map.valueType();

    assertThat(actual).as("Map keys should match").hasSameSizeAs(expected);

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
      case FIXED:
      case BINARY:
      case DECIMAL:
        assertThat(actual).as("Primitive value should be equal to expected").isEqualTo(expected);
        break;
      case STRUCT:
        assertThat(expected).as("Expected should be a Record").isInstanceOf(Record.class);
        assertThat(actual).as("Actual should be a Record").isInstanceOf(Record.class);
        assertEquals(type.asStructType(), (Record) expected, (Record) actual);
        break;
      case LIST:
        assertThat(expected).as("Expected should be a List").isInstanceOf(List.class);
        assertThat(actual).as("Actual should be a List").isInstanceOf(List.class);
        assertEquals(type.asListType(), (List) expected, (List) actual);
        break;
      case MAP:
        assertThat(expected).as("Expected should be a Map").isInstanceOf(Map.class);
        assertThat(actual).as("Actual should be a Map").isInstanceOf(Map.class);
        assertEquals(type.asMapType(), (Map<?, ?>) expected, (Map<?, ?>) actual);
        break;
      default:
        throw new IllegalArgumentException("Not a supported type: " + type);
    }
  }
}
