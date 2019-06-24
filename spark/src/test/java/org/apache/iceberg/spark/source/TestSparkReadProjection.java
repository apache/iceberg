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

package org.apache.iceberg.spark.source;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericData.Record;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.UNION;
import static org.apache.iceberg.Files.localOutput;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestSparkReadProjection extends TestReadProjection {
  private static SparkSession spark = null;

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { "parquet" },
        new Object[] { "avro" }
    };
  }

  public TestSparkReadProjection(String format) {
    super(format);
  }

  @BeforeClass
  public static void startSpark() {
    TestSparkReadProjection.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestSparkReadProjection.spark;
    TestSparkReadProjection.spark = null;
    currentSpark.stop();
  }

  @Override
  protected Record writeAndRead(String desc, Schema writeSchema, Schema readSchema,
                                Record record) throws IOException {
    File parent = temp.newFolder(desc);
    File location = new File(parent, "test");
    File dataFolder = new File(location, "data");
    Assert.assertTrue("mkdirs should succeed", dataFolder.mkdirs());

    FileFormat fileFormat = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));

    File testFile = new File(dataFolder, fileFormat.addExtension(UUID.randomUUID().toString()));

    Table table = TestTables.create(location, desc, writeSchema, PartitionSpec.unpartitioned());
    try {
      // Important: use the table's schema for the rest of the test
      // When tables are created, the column ids are reassigned.
      Schema tableSchema = table.schema();

      switch (fileFormat) {
        case AVRO:
          try (FileAppender<Record> writer = Avro.write(localOutput(testFile))
              .schema(tableSchema)
              .build()) {
            writer.add(record);
          }
          break;

        case PARQUET:
          try (FileAppender<Record> writer = Parquet.write(localOutput(testFile))
              .schema(tableSchema)
              .build()) {
            writer.add(record);
          }
          break;
      }

      DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
          .withRecordCount(100)
          .withFileSizeInBytes(testFile.length())
          .withPath(testFile.toString())
          .build();

      table.newAppend().appendFile(file).commit();

      // rewrite the read schema for the table's reassigned ids
      Map<Integer, Integer> idMapping = Maps.newHashMap();
      for (int id : allIds(writeSchema)) {
        // translate each id to the original schema's column name, then to the new schema's id
        String originalName = writeSchema.findColumnName(id);
        idMapping.put(id, tableSchema.findField(originalName).fieldId());
      }
      Schema expectedSchema = reassignIds(readSchema, idMapping);

      // Set the schema to the expected schema directly to simulate the table schema evolving
      TestTables.replaceMetadata(desc,
          TestTables.readMetadata(desc).updateSchema(expectedSchema, 100));

      Dataset<Row> df = spark.read()
          .format("org.apache.iceberg.spark.source.TestIcebergSource")
          .option("iceberg.table.name", desc)
          .load();

      // convert to Avro using the read schema so that the record schemas match
      return convert(AvroSchemaUtil.convert(readSchema, "table"), df.collectAsList().get(0));

    } finally {
      TestTables.clearTables();
    }
  }

  @SuppressWarnings("unchecked")
  private Object convert(org.apache.avro.Schema schema, Object object) {
    switch (schema.getType()) {
      case RECORD:
        return convert(schema, (Row) object);

      case ARRAY:
        List<Object> convertedList = Lists.newArrayList();
        List<?> list = (List<?>) object;
        for (Object element : list) {
          convertedList.add(convert(schema.getElementType(), element));
        }
        return convertedList;

      case MAP:
        Map<String, Object> convertedMap = Maps.newLinkedHashMap();
        Map<String, ?> map = (Map<String, ?>) object;
        for (Map.Entry<String, ?> entry : map.entrySet()) {
          convertedMap.put(entry.getKey(), convert(schema.getValueType(), entry.getValue()));
        }
        return convertedMap;

      case UNION:
        if (object == null) {
          return null;
        }
        List<org.apache.avro.Schema> types = schema.getTypes();
        if (types.get(0).getType() != NULL) {
          return convert(types.get(0), object);
        } else {
          return convert(types.get(1), object);
        }

      case FIXED:
        Fixed convertedFixed = new Fixed(schema);
        convertedFixed.bytes((byte[]) object);
        return convertedFixed;

      case BYTES:
        return ByteBuffer.wrap((byte[]) object);

      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case STRING:
        return object;

      case NULL:
        return null;

      default:
        throw new UnsupportedOperationException("Not a supported type: " + schema);
    }
  }

  private Record convert(org.apache.avro.Schema schema, Row row) {
    org.apache.avro.Schema schemaToConvert = schema;
    if (schema.getType() == UNION) {
      if (schema.getTypes().get(0).getType() != NULL) {
        schemaToConvert = schema.getTypes().get(0);
      } else {
        schemaToConvert = schema.getTypes().get(1);
      }
    }

    Record record = new Record(schemaToConvert);
    List<org.apache.avro.Schema.Field> fields = schemaToConvert.getFields();
    for (int i = 0; i < fields.size(); i += 1) {
      org.apache.avro.Schema.Field field = fields.get(i);

      org.apache.avro.Schema fieldSchema = field.schema();
      if (fieldSchema.getType() == UNION) {
        if (fieldSchema.getTypes().get(0).getType() != NULL) {
          fieldSchema = fieldSchema.getTypes().get(0);
        } else {
          fieldSchema = fieldSchema.getTypes().get(1);
        }
      }

      switch (fieldSchema.getType()) {
        case RECORD:
          record.put(i, convert(field.schema(), row.getStruct(i)));
          break;
        case ARRAY:
          record.put(i, convert(field.schema(), row.getList(i)));
          break;
        case MAP:
          record.put(i, convert(field.schema(), row.getJavaMap(i)));
          break;
        default:
          record.put(i, convert(field.schema(), row.get(i)));
      }
    }
    return record;
  }

  private List<Integer> allIds(Schema schema) {
    List<Integer> ids = Lists.newArrayList();
    TypeUtil.visit(schema, new TypeUtil.SchemaVisitor<Void>() {
      @Override
      public Void field(Types.NestedField field, Void fieldResult) {
        ids.add(field.fieldId());
        return null;
      }

      @Override
      public Void list(Types.ListType list, Void elementResult) {
        ids.add(list.elementId());
        return null;
      }

      @Override
      public Void map(Types.MapType map, Void keyResult, Void valueResult) {
        ids.add(map.keyId());
        ids.add(map.valueId());
        return null;
      }
    });
    return ids;
  }

  private Schema reassignIds(Schema schema, Map<Integer, Integer> idMapping) {
    return new Schema(TypeUtil.visit(schema, new TypeUtil.SchemaVisitor<Type>() {
      private int mapId(int id) {
        if (idMapping.containsKey(id)) {
          return idMapping.get(id);
        }
        return 1000 + id; // make sure the new IDs don't conflict with reassignment
      }

      @Override
      public Type schema(Schema schema, Type structResult) {
        return structResult;
      }

      @Override
      public Type struct(Types.StructType struct, List<Type> fieldResults) {
        List<Types.NestedField> newFields = Lists.newArrayListWithExpectedSize(fieldResults.size());
        List<Types.NestedField> fields = struct.fields();
        for (int i = 0; i < fields.size(); i += 1) {
          Types.NestedField field = fields.get(i);
          if (field.isOptional()) {
            newFields.add(optional(mapId(field.fieldId()), field.name(), fieldResults.get(i)));
          } else {
            newFields.add(required(mapId(field.fieldId()), field.name(), fieldResults.get(i)));
          }
        }
        return Types.StructType.of(newFields);
      }

      @Override
      public Type field(Types.NestedField field, Type fieldResult) {
        return fieldResult;
      }

      @Override
      public Type list(Types.ListType list, Type elementResult) {
        if (list.isElementOptional()) {
          return Types.ListType.ofOptional(mapId(list.elementId()), elementResult);
        } else {
          return Types.ListType.ofRequired(mapId(list.elementId()), elementResult);
        }
      }

      @Override
      public Type map(Types.MapType map, Type keyResult, Type valueResult) {
        if (map.isValueOptional()) {
          return Types.MapType.ofOptional(
              mapId(map.keyId()), mapId(map.valueId()), keyResult, valueResult);
        } else {
          return Types.MapType.ofRequired(
              mapId(map.keyId()), mapId(map.valueId()), keyResult, valueResult);
        }
      }

      @Override
      public Type primitive(Type.PrimitiveType primitive) {
        return primitive;
      }
    }).asNestedType().asStructType().fields());
  }
}
