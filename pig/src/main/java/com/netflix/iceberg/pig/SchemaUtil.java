/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.pig;

import com.google.common.collect.Lists;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;

import java.io.IOException;
import java.util.List;

import static java.lang.String.format;
import static com.netflix.iceberg.types.Types.ListType;
import static com.netflix.iceberg.types.Types.MapType;
import static com.netflix.iceberg.types.Types.NestedField;
import static com.netflix.iceberg.types.Types.StructType;

public class SchemaUtil {

  public static ResourceSchema convert(Schema icebergSchema) throws IOException {
    ResourceSchema result = new ResourceSchema();
    List<ResourceFieldSchema> fields = Lists.newArrayList();
    for (Types.NestedField nf : icebergSchema.columns()) {
      fields.add(convert(nf));
    }
    result.setFields(fields.toArray(new ResourceSchema.ResourceFieldSchema[0]));
    return result;
  }

  private static ResourceFieldSchema convert(Types.NestedField field) throws IOException {
    ResourceFieldSchema result = new ResourceFieldSchema();
    result.setName(field.name());
    result.setDescription(format("FieldId: %s", field.fieldId()));

    result.setType(convertType(field.type()));

    if (!field.type().isPrimitiveType()) {
      result.setSchema(convertComplex(field));
    }

    return result;
  }

  private static ResourceFieldSchema convert(Type type) throws IOException {
    ResourceFieldSchema result = new ResourceFieldSchema();
    result.setType(convertType(type));

    return result;
  }

  private static byte convertType(Type type) throws IOException {
    switch (type.typeId()) {
      case BOOLEAN:   return DataType.BOOLEAN;
      case INTEGER:   return DataType.INTEGER;
      case LONG:      return DataType.LONG;
      case FLOAT:     return DataType.FLOAT;
      case DOUBLE:    return DataType.DOUBLE;
      case TIMESTAMP: return DataType.CHARARRAY;
      case DATE:      return DataType.CHARARRAY;
      case STRING:    return DataType.CHARARRAY;
      case FIXED:     return DataType.BYTEARRAY;
      case BINARY:    return DataType.BYTEARRAY;
      case DECIMAL:   return DataType.BIGDECIMAL;
      case STRUCT:    return DataType.TUPLE;
      case LIST:      return DataType.BAG;
      case MAP:       return DataType.MAP;
      default:
        throw new FrontendException("Unsupported primitive type:" + type);
    }
  }

  private static ResourceSchema convertComplex(Types.NestedField field) throws IOException {
    ResourceSchema result = new ResourceSchema();

    Type type = field.type();

    switch (type.typeId()) {
      case STRUCT:
        StructType structType = type.asStructType();

        List<ResourceFieldSchema> fields = Lists.newArrayList();

        for (Types.NestedField f : structType.fields()) {
          fields.add(convert(f.type()));
        }

        result.setFields(fields.toArray(new ResourceFieldSchema[0]));

        return result;
      case LIST:
        ListType listType = type.asListType();

        ResourceSchema innerSchema = new ResourceSchema();
        innerSchema.setFields(new ResourceFieldSchema[]{convert(listType.elementType())});

        //Bag elements are tuples
        ResourceFieldSchema tupleSchema = new ResourceFieldSchema();
        tupleSchema.setType(DataType.TUPLE);
        tupleSchema.setSchema(innerSchema);

        result.setFields(new ResourceFieldSchema[]{tupleSchema});

        return result;
      case MAP:
        MapType mapType = type.asMapType();

        if (mapType.keyType().typeId() != Type.TypeID.STRING) {
          throw new FrontendException("Unsupported map key type: " + mapType.keyType());
        }
        result.setFields(new ResourceFieldSchema[]{convert(mapType.valueType())});

        return result;
      default:
        throw new FrontendException("Unsupported complex type: " + field);
    }
  }

  public static Schema project(Schema schema, List<String> requiredFields) {
    List<NestedField> columns = Lists.newArrayList();

    for (String column : requiredFields) {
      columns.add(schema.findField(column));
    }

    return new Schema(columns);
  }

}
