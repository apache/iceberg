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
package org.apache.iceberg.connect.data;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.common.DynMethods.BoundMethod;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utilities {

  private static final Logger LOG = LoggerFactory.getLogger(Utilities.class.getName());
  private static final List<String> HADOOP_CONF_FILES =
      ImmutableList.of("core-site.xml", "hdfs-site.xml", "hive-site.xml");

  public static Catalog loadCatalog(IcebergSinkConfig config) {
    return CatalogUtil.buildIcebergCatalog(
        config.catalogName(), config.catalogProps(), loadHadoopConfig(config));
  }

  // use reflection here to avoid requiring Hadoop as a dependency
  private static Object loadHadoopConfig(IcebergSinkConfig config) {
    Class<?> configClass =
        DynClasses.builder()
            .impl("org.apache.hadoop.hdfs.HdfsConfiguration")
            .impl("org.apache.hadoop.conf.Configuration")
            .orNull()
            .build();

    if (configClass == null) {
      LOG.info("Hadoop not found on classpath, not creating Hadoop config");
      return null;
    }

    try {
      Object result = DynConstructors.builder().hiddenImpl(configClass).build().newInstance();
      BoundMethod addResourceMethod =
          DynMethods.builder("addResource").impl(configClass, URL.class).build(result);
      BoundMethod setMethod =
          DynMethods.builder("set").impl(configClass, String.class, String.class).build(result);

      //  load any config files in the specified config directory
      String hadoopConfDir = config.hadoopConfDir();
      if (hadoopConfDir != null) {
        HADOOP_CONF_FILES.forEach(
            confFile -> {
              Path path = Paths.get(hadoopConfDir, confFile);
              if (Files.exists(path)) {
                try {
                  addResourceMethod.invoke(path.toUri().toURL());
                } catch (IOException e) {
                  LOG.warn("Error adding Hadoop resource {}, resource was not added", path, e);
                }
              }
            });
      }

      // set any Hadoop properties specified in the sink config
      config.hadoopProps().forEach(setMethod::invoke);

      LOG.info("Hadoop config initialized: {}", configClass.getName());
      return result;
    } catch (Exception e) {
      LOG.warn(
          "Hadoop found on classpath but could not create config, proceeding without config", e);
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public static Object extractFromRecordValue(Object recordValue, String fieldName) {
    List<String> fields = Splitter.on('.').splitToList(fieldName);
    if (recordValue instanceof Struct) {
      return valueFromStruct((Struct) recordValue, fields);
    } else if (recordValue instanceof Map) {
      return valueFromMap((Map<String, ?>) recordValue, fields);
    } else {
      throw new UnsupportedOperationException(
          "Cannot extract value from type: " + recordValue.getClass().getName());
    }
  }

  private static Object valueFromStruct(Struct parent, List<String> fields) {
    Struct struct = parent;
    for (int idx = 0; idx < fields.size() - 1; idx++) {
      Object value = fieldValueFromStruct(struct, fields.get(idx));
      if (value == null) {
        return null;
      }
      Preconditions.checkState(value instanceof Struct, "Expected a struct type");
      struct = (Struct) value;
    }
    return fieldValueFromStruct(struct, fields.get(fields.size() - 1));
  }

  private static Object fieldValueFromStruct(Struct struct, String fieldName) {
    Field structField = struct.schema().field(fieldName);
    if (structField == null) {
      return null;
    }
    return struct.get(structField);
  }

  @SuppressWarnings("unchecked")
  private static Object valueFromMap(Map<String, ?> parent, List<String> fields) {
    Map<String, ?> map = parent;
    for (int idx = 0; idx < fields.size() - 1; idx++) {
      Object value = map.get(fields.get(idx));
      if (value == null) {
        return null;
      }
      Preconditions.checkState(value instanceof Map, "Expected a map type");
      map = (Map<String, ?>) value;
    }
    return map.get(fields.get(fields.size() - 1));
  }

  public static TaskWriter<Record> createTableWriter(
      Table table, String tableName, IcebergSinkConfig config) {
    Map<String, String> tableProps = Maps.newHashMap(table.properties());
    tableProps.putAll(config.writeProps());

    String formatStr =
        tableProps.getOrDefault(
            TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    FileFormat format = FileFormat.fromString(formatStr);

    long targetFileSize =
        PropertyUtil.propertyAsLong(
            tableProps,
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
            TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

    Set<Integer> identifierFieldIds = table.schema().identifierFieldIds();

    // override the identifier fields if the config is set
    List<String> idCols = config.tableConfig(tableName).idColumns();
    if (!idCols.isEmpty()) {
      identifierFieldIds =
          idCols.stream()
              .map(
                  colName -> {
                    NestedField field = table.schema().findField(colName);
                    if (field == null) {
                      throw new IllegalArgumentException("ID column not found: " + colName);
                    }
                    return field.fieldId();
                  })
              .collect(Collectors.toSet());
    }

    FileAppenderFactory<Record> appenderFactory;
    if (identifierFieldIds == null || identifierFieldIds.isEmpty()) {
      appenderFactory =
          new GenericAppenderFactory(table.schema(), table.spec(), null, null, null)
              .setAll(tableProps);
    } else {
      appenderFactory =
          new GenericAppenderFactory(
                  table.schema(),
                  table.spec(),
                  Ints.toArray(identifierFieldIds),
                  TypeUtil.select(table.schema(), Sets.newHashSet(identifierFieldIds)),
                  null)
              .setAll(tableProps);
    }

    // (partition ID + task ID + operation ID) must be unique
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
            .defaultSpec(table.spec())
            .operationId(UUID.randomUUID().toString())
            .format(format)
            .build();

    TaskWriter<Record> writer;
    if (table.spec().isUnpartitioned()) {
      writer =
          new UnpartitionedWriter<>(
              table.spec(), format, appenderFactory, fileFactory, table.io(), targetFileSize);
    } else {
      writer =
          new PartitionedAppendWriter(
              table.spec(),
              format,
              appenderFactory,
              fileFactory,
              table.io(),
              targetFileSize,
              table.schema());
    }
    return writer;
  }

  private Utilities() {}
}
