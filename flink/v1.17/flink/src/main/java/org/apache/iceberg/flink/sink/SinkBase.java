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
package org.apache.iceberg.flink.sink;

import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION;
import static org.apache.iceberg.TableProperties.AVRO_COMPRESSION_LEVEL;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION;
import static org.apache.iceberg.TableProperties.ORC_COMPRESSION_STRATEGY;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION;
import static org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_LEVEL;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.FlinkWriteConf;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.SerializableSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SinkBase implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(SinkBase.class);

  private transient SinkBuilder builder;
  private List<Integer> equalityFieldIds;
  private RowType flinkRowType;
  // Can not be serialized, so we have to be careful and serialize specific fields when needed
  private transient FlinkWriteConf flinkWriteConf;
  private Map<String, String> writeProperties;
  private SerializableSupplier<Table> tableSupplier;

  SinkBase(SinkBuilder builder) {
    this.builder = builder;
  }

  SinkBuilder builder() {
    return builder;
  }

  FlinkWriteConf flinkWriteConf() {
    return flinkWriteConf;
  }

  Map<String, String> writeProperties() {
    return writeProperties;
  }

  SerializableSupplier<Table> tableSupplier() {
    return tableSupplier;
  }

  RowType flinkRowType() {
    return flinkRowType;
  }

  List<Integer> equalityFieldIds() {
    return equalityFieldIds;
  }

  void validate() {
    Preconditions.checkArgument(
        builder.inputCreator() != null,
        "Please use forRowData() or forMapperOutputType() to initialize the input DataStream.");
    Preconditions.checkNotNull(builder.tableLoader(), "Table loader shouldn't be null");

    // Set the table if it is not yet set in the builder, so we can do the equalityId checks
    builder.table(checkAndGetTable(builder.tableLoader(), builder.table()));

    // Validate the equality fields and partition fields if we enable the upsert mode.
    this.equalityFieldIds = checkAndGetEqualityFieldIds();

    // Init the `flinkWriteConf` here, so we can do the checks
    this.flinkWriteConf =
        new FlinkWriteConf(builder.table(), builder.writeOptions(), builder.readableConfig());
    if (flinkWriteConf.upsertMode()) {
      Preconditions.checkState(
          !flinkWriteConf.overwriteMode(),
          "OVERWRITE mode shouldn't be enable when configuring to use UPSERT data stream.");
      Preconditions.checkState(
          !equalityFieldIds.isEmpty(),
          "Equality field columns shouldn't be empty when configuring to use UPSERT data stream.");
      if (!builder.table().spec().isUnpartitioned()) {
        for (PartitionField partitionField : builder.table().spec().fields()) {
          Preconditions.checkState(
              equalityFieldIds.contains(partitionField.sourceId()),
              "In UPSERT mode, partition field '%s' should be included in equality fields: '%s'",
              partitionField,
              builder.equalityFieldColumns());
        }
      }
    }
  }

  void init() {
    this.writeProperties =
        writeProperties(builder.table(), flinkWriteConf.dataFileFormat(), flinkWriteConf);
    this.flinkRowType = toFlinkRowType(builder.table().schema(), builder.tableSchema());

    Duration tableRefreshInterval = flinkWriteConf.tableRefreshInterval();

    if (tableRefreshInterval != null) {
      this.tableSupplier =
          new CachingTableSupplier(
              (SerializableTable) builder.table(), builder.tableLoader(), tableRefreshInterval);
    } else {
      this.tableSupplier = new SimpleTableSupplier((SerializableTable) builder.table());
    }
  }

  private static Table checkAndGetTable(TableLoader tableLoader, Table table) {
    if (table == null) {
      if (!tableLoader.isOpen()) {
        tableLoader.open();
      }

      try (TableLoader loader = tableLoader) {
        return SerializableTable.copyOf(loader.loadTable());
      } catch (IOException e) {
        throw new UncheckedIOException(
            "Failed to load iceberg table from table loader: " + tableLoader, e);
      }
    }

    return SerializableTable.copyOf(table);
  }

  List<Integer> checkAndGetEqualityFieldIds() {
    List<Integer> fieldIds = Lists.newArrayList(builder.table().schema().identifierFieldIds());
    if (builder.equalityFieldColumns() != null && !builder.equalityFieldColumns().isEmpty()) {
      Set<Integer> equalityFieldSet =
          Sets.newHashSetWithExpectedSize(builder.equalityFieldColumns().size());
      for (String column : builder.equalityFieldColumns()) {
        org.apache.iceberg.types.Types.NestedField field =
            builder.table().schema().findField(column);
        Preconditions.checkNotNull(
            field,
            "Missing required equality field column '%s' in table schema %s",
            column,
            builder.table().schema());
        equalityFieldSet.add(field.fieldId());
      }

      if (!equalityFieldSet.equals(builder.table().schema().identifierFieldIds())) {
        LOG.warn(
            "The configured equality field column IDs {} are not matched with the schema identifier field IDs"
                + " {}, use job specified equality field columns as the equality fields by default.",
            equalityFieldSet,
            builder.table().schema().identifierFieldIds());
      }
      fieldIds = Lists.newArrayList(equalityFieldSet);
    }
    return fieldIds;
  }

  static RowType toFlinkRowType(Schema schema, TableSchema requestedSchema) {
    if (requestedSchema != null) {
      // Convert the flink schema to iceberg schema firstly, then reassign ids to match the existing
      // iceberg schema.
      Schema writeSchema = TypeUtil.reassignIds(FlinkSchemaUtil.convert(requestedSchema), schema);
      TypeUtil.validateWriteSchema(schema, writeSchema, true, true);

      // We use this flink schema to read values from RowData. The flink's TINYINT and SMALLINT will
      // be promoted to iceberg INTEGER, that means if we use iceberg's table schema to read TINYINT
      // (backend by 1 'byte'), we will read 4 bytes rather than 1 byte, it will mess up the byte
      // array in
      // BinaryRowData. So here we must use flink schema.
      return (RowType) requestedSchema.toRowDataType().getLogicalType();
    } else {
      return FlinkSchemaUtil.convert(schema);
    }
  }

  /**
   * Based on the {@link FileFormat} overwrites the table level compression properties for the table
   * write.
   *
   * @param table The table to get the table level settings
   * @param format The FileFormat to use
   * @param conf The write configuration
   * @return The properties to use for writing
   */
  public static Map<String, String> writeProperties(
      Table table, FileFormat format, FlinkWriteConf conf) {
    Map<String, String> writeProperties = Maps.newHashMap(table.properties());

    switch (format) {
      case PARQUET:
        writeProperties.put(PARQUET_COMPRESSION, conf.parquetCompressionCodec());
        String parquetCompressionLevel = conf.parquetCompressionLevel();
        if (parquetCompressionLevel != null) {
          writeProperties.put(PARQUET_COMPRESSION_LEVEL, parquetCompressionLevel);
        }

        break;
      case AVRO:
        writeProperties.put(AVRO_COMPRESSION, conf.avroCompressionCodec());
        String avroCompressionLevel = conf.avroCompressionLevel();
        if (avroCompressionLevel != null) {
          writeProperties.put(AVRO_COMPRESSION_LEVEL, conf.avroCompressionLevel());
        }

        break;
      case ORC:
        writeProperties.put(ORC_COMPRESSION, conf.orcCompressionCodec());
        writeProperties.put(ORC_COMPRESSION_STRATEGY, conf.orcCompressionStrategy());
        break;
      default:
        throw new IllegalArgumentException(String.format("Unknown file format %s", format));
    }

    return ImmutableMap.copyOf(writeProperties);
  }

  DataStream<RowData> distributeDataStream(DataStream<RowData> input) {
    DistributionMode mode = flinkWriteConf.distributionMode();
    Schema schema = builder.table().schema();
    PartitionSpec spec = builder.table().spec();
    LOG.info("Write distribution mode is '{}'", mode.modeName());
    switch (mode) {
      case NONE:
        if (equalityFieldIds.isEmpty()) {
          return input;
        } else {
          LOG.info("Distribute rows by equality fields, because there are equality fields set");
          return input.keyBy(new EqualityFieldKeySelector(schema, flinkRowType, equalityFieldIds));
        }

      case HASH:
        if (equalityFieldIds.isEmpty()) {
          if (builder.table().spec().isUnpartitioned()) {
            LOG.warn(
                "Fallback to use 'none' distribution mode, because there are no equality fields set "
                    + "and table is unpartitioned");
            return input;
          } else {
            if (BucketPartitionerUtil.hasOneBucketField(spec)) {
              return input.partitionCustom(
                  new BucketPartitioner(spec),
                  new BucketPartitionKeySelector(spec, schema, flinkRowType));
            } else {
              return input.keyBy(new PartitionKeySelector(spec, schema, flinkRowType));
            }
          }
        } else {
          if (spec.isUnpartitioned()) {
            LOG.info(
                "Distribute rows by equality fields, because there are equality fields set "
                    + "and table is unpartitioned");
            return input.keyBy(
                new EqualityFieldKeySelector(schema, flinkRowType, equalityFieldIds));
          } else {
            for (PartitionField partitionField : spec.fields()) {
              Preconditions.checkState(
                  equalityFieldIds.contains(partitionField.sourceId()),
                  "In 'hash' distribution mode with equality fields set, partition field '%s' "
                      + "should be included in equality fields: '%s'",
                  partitionField,
                  builder.equalityFieldColumns());
            }
            return input.keyBy(new PartitionKeySelector(spec, schema, flinkRowType));
          }
        }

      case RANGE:
        if (equalityFieldIds.isEmpty()) {
          LOG.warn(
              "Fallback to use 'none' distribution mode, because there are no equality fields set "
                  + "and {}=range is not supported yet in flink",
              WRITE_DISTRIBUTION_MODE);
          return input;
        } else {
          LOG.info(
              "Distribute rows by equality fields, because there are equality fields set "
                  + "and{}=range is not supported yet in flink",
              WRITE_DISTRIBUTION_MODE);
          return input.keyBy(new EqualityFieldKeySelector(schema, flinkRowType, equalityFieldIds));
        }

      default:
        throw new RuntimeException("Unrecognized " + WRITE_DISTRIBUTION_MODE + ": " + mode);
    }
  }

  static String prefixIfNotNull(String uidPrefix, String suffix) {
    return uidPrefix != null ? uidPrefix + "-" + suffix : suffix;
  }
}
