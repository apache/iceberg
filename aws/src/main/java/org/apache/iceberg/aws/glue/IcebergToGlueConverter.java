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

package org.apache.iceberg.aws.glue;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.Schedule;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;

class IcebergToGlueConverter {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergToGlueConverter.class);

  private IcebergToGlueConverter() {
  }

  private static final Pattern GLUE_DB_PATTERN = Pattern.compile("^[a-z0-9_]{1,252}$");
  private static final Pattern GLUE_TABLE_PATTERN = Pattern.compile("^[a-z0-9_]{1,255}$");
  public static final String GLUE_DB_LOCATION_KEY = "location";
  public static final String GLUE_DB_DESCRIPTION_KEY = "comment";
  public static final String ICEBERG_FIELD_USAGE = "iceberg.field.usage";
  public static final String ICEBERG_FIELD_TYPE_TYPE_ID = "iceberg.field.type.typeid";
  public static final String ICEBERG_FIELD_TYPE_STRING = "iceberg.field.type.string";
  public static final String ICEBERG_FIELD_ID = "iceberg.field.id";
  public static final String ICEBERG_FIELD_OPTIONAL = "iceberg.field.optional";
  public static final String ICEBERG_PARTITION_TRANSFORM = "iceberg.partition.transform";
  public static final String ICEBERG_PARTITION_FIELD_ID = "iceberg.partition.field-id";
  public static final String ICEBERG_PARTITION_SOURCE_ID = "iceberg.partition.source-id";
  public static final String SCHEMA_COLUMN = "schema-column";
  public static final String SCHEMA_SUBFIELD = "schema-subfield";
  public static final String PARTITION_FIELD = "partition-field";

  /**
   * A Glue database name cannot be longer than 252 characters.
   * The only acceptable characters are lowercase letters, numbers, and the underscore character.
   * More details: https://docs.aws.amazon.com/athena/latest/ug/glue-best-practices.html
   * @param namespace namespace
   * @return if namespace can be accepted by Glue
   */
  static boolean isValidNamespace(Namespace namespace) {
    if (namespace.levels().length != 1) {
      return false;
    }
    String dbName = namespace.level(0);
    return dbName != null && GLUE_DB_PATTERN.matcher(dbName).find();
  }

  /**
   * Validate if an Iceberg namespace is valid in Glue
   * @param namespace namespace
   * @throws NoSuchNamespaceException if namespace is not valid in Glue
   */
  static void validateNamespace(Namespace namespace) {
    ValidationException.check(isValidNamespace(namespace), "Cannot convert namespace %s to Glue database name, " +
        "because it must be 1-252 chars of lowercase letters, numbers, underscore", namespace);
  }

  /**
   * Validate and convert Iceberg namespace to Glue database name
   * @param namespace Iceberg namespace
   * @return database name
   */
  static String toDatabaseName(Namespace namespace) {
    validateNamespace(namespace);
    return namespace.level(0);
  }

  /**
   * Validate and get Glue database name from Iceberg TableIdentifier
   * @param tableIdentifier Iceberg table identifier
   * @return database name
   */
  static String getDatabaseName(TableIdentifier tableIdentifier) {
    return toDatabaseName(tableIdentifier.namespace());
  }

  /**
   * Validate and convert Iceberg name to Glue DatabaseInput
   * @param namespace Iceberg namespace
   * @param metadata metadata map
   * @return Glue DatabaseInput
   */
  static DatabaseInput toDatabaseInput(Namespace namespace, Map<String, String> metadata) {
    DatabaseInput.Builder builder = DatabaseInput.builder().name(toDatabaseName(namespace));
    Map<String, String> parameters = Maps.newHashMap();
    metadata.forEach((k, v) -> {
      if (GLUE_DB_DESCRIPTION_KEY.equals(k)) {
        builder.description(v);
      } else if (GLUE_DB_LOCATION_KEY.equals(k)) {
        builder.locationUri(v);
      } else {
        parameters.put(k, v);
      }
    });

    return builder.parameters(parameters).build();
  }

  /**
   * A Glue table name cannot be longer than 255 characters.
   * The only acceptable characters are lowercase letters, numbers, and the underscore character.
   * More details: https://docs.aws.amazon.com/athena/latest/ug/glue-best-practices.html
   * @param tableName table name
   * @return if a table name can be accepted by Glue
   */
  static boolean isValidTableName(String tableName) {
    return tableName != null && GLUE_TABLE_PATTERN.matcher(tableName).find();
  }

  /**
   * Validate if a table name is valid in Glue
   * @param tableName table name
   * @throws NoSuchTableException if table name not valid in Glue
   */
  static void validateTableName(String tableName) {
    ValidationException.check(isValidTableName(tableName), "Cannot use %s as Glue table name, " +
        "because it must be 1-255 chars of lowercase letters, numbers, underscore", tableName);
  }

  /**
   * Validate and get Glue table name from Iceberg TableIdentifier
   * @param tableIdentifier table identifier
   * @return table name
   */
  static String getTableName(TableIdentifier tableIdentifier) {
    validateTableName(tableIdentifier.name());
    return tableIdentifier.name();
  }

  /**
   * Validate Iceberg TableIdentifier is valid in Glue
   * @param tableIdentifier Iceberg table identifier
   */
  static void validateTableIdentifier(TableIdentifier tableIdentifier) {
    validateNamespace(tableIdentifier.namespace());
    validateTableName(tableIdentifier.name());
  }

  /**
   * Set Glue table input information based on Iceberg table metadata.
   * <p>
   * A best-effort conversion of Iceberg metadata to Glue table is performed to display Iceberg information in Glue,
   * but such information is only intended for informational human read access through tools like UI or CLI,
   * and should never be used by any query processing engine to infer information like schema, partition spec, etc.
   * The source of truth is stored in the actual Iceberg metadata file defined by the metadata_location table property.
   * @param tableInputBuilder Glue TableInput builder
   * @param metadata Iceberg table metadata
   */
  static void setTableInputInformation(TableInput.Builder tableInputBuilder, TableMetadata metadata) {
    try {
      tableInputBuilder
          .storageDescriptor(StorageDescriptor.builder()
              .location(safeString(metadata.location()))
              .columns(toColumns(metadata))
              .build());
    } catch (RuntimeException e) {
      LOG.warn("Encountered unexpected exception while converting Iceberg metadata to Glue table information", e);
    }
  }

  /**
   * Converting from an Iceberg type to a type string that can be displayed in Glue.
   * <p>
   * Such conversion is only used for informational purpose,
   * DO NOT reference this method for any actual data processing type conversion.
   *
   * @param type Iceberg type
   * @return type string
   */
  private static String toTypeString(Type type) {
    switch (type.typeId()) {
      case BOOLEAN:
        return "boolean";
      case INTEGER:
        return "int";
      case LONG:
        return "bigint";
      case FLOAT:
        return "float";
      case DOUBLE:
        return "double";
      case DATE:
        return "date";
      case TIME:
      case STRING:
      case UUID:
        return "string";
      case TIMESTAMP:
        return "timestamp";
      case FIXED:
      case BINARY:
        return "binary";
      case DECIMAL:
        final Types.DecimalType decimalType = (Types.DecimalType) type;
        return String.format("decimal(%s,%s)", decimalType.precision(), decimalType.scale());
      case STRUCT:
        final Types.StructType structType = type.asStructType();
        final String nameToType = structType.fields().stream()
            .map(f -> String.format("%s:%s", f.name(), toTypeString(f.type())))
            .collect(Collectors.joining(","));
        return String.format("struct<%s>", nameToType);
      case LIST:
        final Types.ListType listType = type.asListType();
        return String.format("array<%s>", toTypeString(listType.elementType()));
      case MAP:
        final Types.MapType mapType = type.asMapType();
        return String.format("map<%s,%s>", toTypeString(mapType.keyType()), toTypeString(mapType.valueType()));
      default:
        return type.typeId().name().toLowerCase(Locale.ENGLISH);
    }
  }

  private static List<Column> toColumns(TableMetadata metadata) {
    List<Column> columns = Lists.newArrayList();
    Set<String> addedNames = Sets.newHashSet();
    // Add schema-column fields
    for (NestedField field : metadata.schema().columns()) {
      addColumnWithDedupe(columns, addedNames, field, field.name(), SCHEMA_COLUMN);
    }
    // Add schema-subfield
    for (String fieldName : TypeUtil.indexNameById(metadata.schema().asStruct()).values()) {
      NestedField field = metadata.schema().findField(fieldName);
      if (field != null) {
        addColumnWithDedupe(columns, addedNames, field, fieldName, SCHEMA_SUBFIELD);
      }
    }
    // Add partition-field
    for (PartitionField partitionField : metadata.spec().fields()) {
      addPartitionColumnWithDedupe(columns, addedNames, partitionField, metadata.spec().schema());
    }
    return columns;
  }

  private static void addColumnWithDedupe(List<Column> columns, Set<String> dedupe,
                                          NestedField field, String fieldName, String usage) {
    if (!dedupe.contains(fieldName)) {
      columns.add(Column.builder()
          .name(fieldName)
          .type(toTypeString(field.type()))
          .comment(field.doc())
          .parameters(convertToParameters(usage, field))
          .build());
      dedupe.add(fieldName);
    }
  }

  private static void addPartitionColumnWithDedupe(List<Column> columns, Set<String> dedupe,
                                                   PartitionField partitionField, Schema schema) {
    String typeId = null;
    String typeString = null;
    try {
      Type type = partitionField.transform()
          .getResultType(schema.findField(partitionField.sourceId()).type());
      typeId = type.typeId().name();
      typeString = toTypeString(type);
    } catch (RuntimeException e) {
      LOG.error("Failed to convert partition field to column", e);
    }

    // avoid identity partition field name collide with schema columns
    String fieldName = partitionField.name();
    if (partitionField.transform().isIdentity()) {
      fieldName += "_identity";
    }

    if (!dedupe.contains(fieldName)) {
      columns.add(Column.builder()
          .name(fieldName)
          .type(typeString)
          .parameters(convertToPartitionFieldParameters(typeId, typeString, partitionField))
          .build());
      dedupe.add(fieldName);
    }
  }

  private static Map<String, String> convertToParameters(String fieldUsage, NestedField field) {
    return ImmutableMap.of(ICEBERG_FIELD_USAGE, fieldUsage,
        ICEBERG_FIELD_TYPE_TYPE_ID, safeString(field.type().typeId().toString()),
        ICEBERG_FIELD_TYPE_STRING, safeString(toTypeString(field.type())),
        ICEBERG_FIELD_ID, Integer.toString(field.fieldId()),
        ICEBERG_FIELD_OPTIONAL, Boolean.toString(field.isOptional())
    );
  }

  private static Map<String, String> convertToPartitionFieldParameters(String typeId, String typeString,
                                                                       PartitionField partitionField) {
    return ImmutableMap.<String, String>builder()
        .put(ICEBERG_FIELD_USAGE, PARTITION_FIELD)
        .put(ICEBERG_FIELD_TYPE_TYPE_ID, safeString(typeId))
        .put(ICEBERG_FIELD_TYPE_STRING, safeString(typeString))
        .put(ICEBERG_FIELD_ID, Integer.toString(partitionField.fieldId()))
        .put(ICEBERG_PARTITION_TRANSFORM, partitionField.transform().toString())
        .put(ICEBERG_PARTITION_FIELD_ID, Integer.toString(partitionField.fieldId()))
        .put(ICEBERG_PARTITION_SOURCE_ID, Integer.toString(partitionField.sourceId()))
        .build();
  }

  private static String safeString(String s) {
    return s != null ? s : "unknown";
  }
}
