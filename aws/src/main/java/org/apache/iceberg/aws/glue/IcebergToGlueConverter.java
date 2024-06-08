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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

class IcebergToGlueConverter {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergToGlueConverter.class);

  private IcebergToGlueConverter() {}

  private static final Pattern GLUE_DB_PATTERN = Pattern.compile("^[a-z0-9_]{1,252}$");
  private static final Pattern GLUE_TABLE_PATTERN = Pattern.compile("^[a-z0-9_]{1,255}$");
  public static final String GLUE_DB_LOCATION_KEY = "location";
  // Utilized for defining descriptions at both the Glue database and table levels.
  public static final String GLUE_DESCRIPTION_KEY = "comment";
  public static final String ICEBERG_FIELD_ID = "iceberg.field.id";
  public static final String ICEBERG_FIELD_OPTIONAL = "iceberg.field.optional";
  public static final String ICEBERG_FIELD_CURRENT = "iceberg.field.current";
  private static final List<String> ADDITIONAL_LOCATION_PROPERTIES =
      ImmutableList.of(
          TableProperties.WRITE_DATA_LOCATION,
          TableProperties.WRITE_METADATA_LOCATION,
          TableProperties.OBJECT_STORE_PATH,
          TableProperties.WRITE_FOLDER_STORAGE_LOCATION);

  // Attempt to set additionalLocations if available on the given AWS SDK version
  private static final DynMethods.UnboundMethod SET_ADDITIONAL_LOCATIONS =
      DynMethods.builder("additionalLocations")
          .hiddenImpl(
              "software.amazon.awssdk.services.glue.model.StorageDescriptor$Builder",
              Collection.class)
          .orNoop()
          .build();

  /**
   * A Glue database name cannot be longer than 252 characters. The only acceptable characters are
   * lowercase letters, numbers, and the underscore character. More details:
   * https://docs.aws.amazon.com/athena/latest/ug/glue-best-practices.html
   *
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
   *
   * @param namespace namespace
   * @throws NoSuchNamespaceException if namespace is not valid in Glue
   */
  static void validateNamespace(Namespace namespace) {
    ValidationException.check(
        isValidNamespace(namespace),
        "Cannot convert namespace %s to Glue database name, "
            + "because it must be 1-252 chars of lowercase letters, numbers, underscore",
        namespace);
  }

  /**
   * Validate and convert Iceberg namespace to Glue database name
   *
   * @param namespace Iceberg namespace
   * @param skipNameValidation should skip name validation
   * @return database name
   */
  static String toDatabaseName(Namespace namespace, boolean skipNameValidation) {
    if (!skipNameValidation) {
      validateNamespace(namespace);
    }

    return namespace.level(0);
  }

  /**
   * Validate and get Glue database name from Iceberg TableIdentifier
   *
   * @param tableIdentifier Iceberg table identifier
   * @param skipNameValidation should skip name validation
   * @return database name
   */
  static String getDatabaseName(TableIdentifier tableIdentifier, boolean skipNameValidation) {
    return toDatabaseName(tableIdentifier.namespace(), skipNameValidation);
  }

  /**
   * Validate and convert Iceberg name to Glue DatabaseInput
   *
   * @param namespace Iceberg namespace
   * @param metadata metadata map
   * @param skipNameValidation should skip name validation
   * @return Glue DatabaseInput
   */
  static DatabaseInput toDatabaseInput(
      Namespace namespace, Map<String, String> metadata, boolean skipNameValidation) {
    DatabaseInput.Builder builder =
        DatabaseInput.builder().name(toDatabaseName(namespace, skipNameValidation));
    Map<String, String> parameters = Maps.newHashMap();
    metadata.forEach(
        (k, v) -> {
          if (GLUE_DESCRIPTION_KEY.equals(k)) {
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
   * A Glue table name cannot be longer than 255 characters. The only acceptable characters are
   * lowercase letters, numbers, and the underscore character. More details:
   * https://docs.aws.amazon.com/athena/latest/ug/glue-best-practices.html
   *
   * @param tableName table name
   * @return if a table name can be accepted by Glue
   */
  static boolean isValidTableName(String tableName) {
    return tableName != null && GLUE_TABLE_PATTERN.matcher(tableName).find();
  }

  /**
   * Validate if a table name is valid in Glue
   *
   * @param tableName table name
   * @throws NoSuchTableException if table name not valid in Glue
   */
  static void validateTableName(String tableName) {
    ValidationException.check(
        isValidTableName(tableName),
        "Cannot use %s as Glue table name, "
            + "because it must be 1-255 chars of lowercase letters, numbers, underscore",
        tableName);
  }

  /**
   * Validate and get Glue table name from Iceberg TableIdentifier
   *
   * @param tableIdentifier table identifier
   * @param skipNameValidation should skip name validation
   * @return table name
   */
  static String getTableName(TableIdentifier tableIdentifier, boolean skipNameValidation) {
    if (!skipNameValidation) {
      validateTableName(tableIdentifier.name());
    }

    return tableIdentifier.name();
  }

  /**
   * Set Glue table input information based on Iceberg table metadata.
   *
   * <p>A best-effort conversion of Iceberg metadata to Glue table is performed to display Iceberg
   * information in Glue, but such information is only intended for informational human read access
   * through tools like UI or CLI, and should never be used by any query processing engine to infer
   * information like schema, partition spec, etc. The source of truth is stored in the actual
   * Iceberg metadata file defined by the metadata_location table property.
   *
   * @param tableInputBuilder Glue TableInput builder
   * @param metadata Iceberg table metadata
   */
  static void setTableInputInformation(
      TableInput.Builder tableInputBuilder, TableMetadata metadata) {
    setTableInputInformation(tableInputBuilder, metadata, null);
  }

  /**
   * Set Glue table input information based on Iceberg table metadata, optionally preserving
   * comments from an existing Glue table's columns.
   *
   * <p>A best-effort conversion of Iceberg metadata to Glue table is performed to display Iceberg
   * information in Glue, but such information is only intended for informational human read access
   * through tools like UI or CLI, and should never be used by any query processing engine to infer
   * information like schema, partition spec, etc. The source of truth is stored in the actual
   * Iceberg metadata file defined by the metadata_location table property.
   *
   * <p>If an existing Glue table is provided, the comments from its columns will be preserved in
   * the resulting Glue TableInput. This is useful when updating an existing Glue table to retain
   * any user-defined comments on the columns.
   *
   * @param tableInputBuilder Glue TableInput builder
   * @param metadata Iceberg table metadata
   * @param existingTable optional existing Glue table, used to preserve column comments
   */
  static void setTableInputInformation(
      TableInput.Builder tableInputBuilder, TableMetadata metadata, Table existingTable) {
    try {
      Map<String, String> properties = metadata.properties();
      StorageDescriptor.Builder storageDescriptor = StorageDescriptor.builder();
      if (!SET_ADDITIONAL_LOCATIONS.isNoop()) {
        SET_ADDITIONAL_LOCATIONS.invoke(
            storageDescriptor,
            ADDITIONAL_LOCATION_PROPERTIES.stream()
                .map(properties::get)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
      }

      String description = properties.get(GLUE_DESCRIPTION_KEY);
      if (description != null) {
        tableInputBuilder.description(description);
      } else if (existingTable != null) {
        Optional.ofNullable(existingTable.description()).ifPresent(tableInputBuilder::description);
      }

      Map<String, String> existingColumnMap = null;
      if (existingTable != null) {
        List<Column> existingColumns = existingTable.storageDescriptor().columns();
        existingColumnMap =
            existingColumns.stream().filter(column -> column.comment() != null).collect(Collectors.toMap(Column::name, Column::comment));
      } else {
        existingColumnMap = Collections.emptyMap();
      }
      List<Column> columns = toColumns(metadata, existingColumnMap);

      tableInputBuilder.storageDescriptor(
          storageDescriptor.location(metadata.location()).columns(columns).build());
    } catch (RuntimeException e) {
      LOG.warn(
          "Encountered unexpected exception while converting Iceberg metadata to Glue table information",
          e);
    }
  }

  /**
   * Converting from an Iceberg type to a type string that can be displayed in Glue.
   *
   * <p>Such conversion is only used for informational purpose, DO NOT reference this method for any
   * actual data processing type conversion.
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
        final String nameToType =
            structType.fields().stream()
                .map(f -> String.format("%s:%s", f.name(), toTypeString(f.type())))
                .collect(Collectors.joining(","));
        return String.format("struct<%s>", nameToType);
      case LIST:
        final Types.ListType listType = type.asListType();
        return String.format("array<%s>", toTypeString(listType.elementType()));
      case MAP:
        final Types.MapType mapType = type.asMapType();
        return String.format(
            "map<%s,%s>", toTypeString(mapType.keyType()), toTypeString(mapType.valueType()));
      default:
        return type.typeId().name().toLowerCase(Locale.ENGLISH);
    }
  }

  private static List<Column> toColumns(TableMetadata metadata, Map<String, String> existingColumnMap) {
    List<Column> columns = Lists.newArrayList();
    Set<String> addedNames = Sets.newHashSet();

    for (NestedField field : metadata.schema().columns()) {
      addColumnWithDedupe(columns, addedNames, field, true /* is current */, existingColumnMap);
    }

    for (Schema schema : metadata.schemas()) {
      if (schema.schemaId() != metadata.currentSchemaId()) {
        for (NestedField field : schema.columns()) {
          addColumnWithDedupe(columns, addedNames, field, false /* is not current */,existingColumnMap);
        }
      }
    }

    return columns;
  }

  private static void addColumnWithDedupe(
      List<Column> columns, Set<String> dedupe, NestedField field, boolean isCurrent, Map<String, String> existingColumnMap) {
    if (!dedupe.contains(field.name())) {
      Column.Builder builder = Column.builder()
        .name(field.name())
        .type(toTypeString(field.type()))
        .parameters(
          ImmutableMap.of(
              ICEBERG_FIELD_ID, Integer.toString(field.fieldId()),
              ICEBERG_FIELD_OPTIONAL, Boolean.toString(field.isOptional()),
              ICEBERG_FIELD_CURRENT, Boolean.toString(isCurrent)));

      if (field.doc() != null && !field.doc().isEmpty()) {
        builder.comment(field.doc());
      } else if (existingColumnMap != null && existingColumnMap.containsKey(field.name())) {
        builder.comment(existingColumnMap.get(field.name()));
      }

      columns.add(builder.build());
      dedupe.add(field.name());
    }
  }
}
