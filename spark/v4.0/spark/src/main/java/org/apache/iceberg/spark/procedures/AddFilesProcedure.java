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
package org.apache.iceberg.spark.procedures;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.SparkTableUtil.SparkPartition;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.procedures.BoundProcedure;
import org.apache.spark.sql.connector.catalog.procedures.ProcedureParameter;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

class AddFilesProcedure extends BaseProcedure {

  static final String NAME = "add_files";

  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter SOURCE_TABLE_PARAM =
      requiredInParameter("source_table", DataTypes.StringType);
  private static final ProcedureParameter PARTITION_FILTER_PARAM =
      optionalInParameter("partition_filter", STRING_MAP);
  private static final ProcedureParameter CHECK_DUPLICATE_FILES_PARAM =
      optionalInParameter("check_duplicate_files", DataTypes.BooleanType);
  private static final ProcedureParameter PARALLELISM =
      optionalInParameter("parallelism", DataTypes.IntegerType);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        TABLE_PARAM,
        SOURCE_TABLE_PARAM,
        PARTITION_FILTER_PARAM,
        CHECK_DUPLICATE_FILES_PARAM,
        PARALLELISM
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("added_files_count", DataTypes.LongType, false, Metadata.empty()),
            new StructField("changed_partition_count", DataTypes.LongType, true, Metadata.empty()),
          });

  private AddFilesProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  public static SparkProcedures.ProcedureBuilder builder() {
    return new BaseProcedure.Builder<AddFilesProcedure>() {
      @Override
      protected AddFilesProcedure doBuild() {
        return new AddFilesProcedure(tableCatalog());
      }
    };
  }

  @Override
  public BoundProcedure bind(StructType inputType) {
    return this;
  }

  @Override
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public Iterator<Scan> call(InternalRow args) {
    ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);

    Identifier tableIdent = input.ident(TABLE_PARAM);

    CatalogPlugin sessionCat = spark().sessionState().catalogManager().v2SessionCatalog();
    Identifier sourceIdent = input.ident(SOURCE_TABLE_PARAM, sessionCat);

    Map<String, String> partitionFilter =
        input.asStringMap(PARTITION_FILTER_PARAM, ImmutableMap.of());

    boolean checkDuplicateFiles = input.asBoolean(CHECK_DUPLICATE_FILES_PARAM, true);

    int parallelism = input.asInt(PARALLELISM, 1);
    Preconditions.checkArgument(parallelism > 0, "Parallelism should be larger than 0");

    return asScanIterator(
        OUTPUT_TYPE,
        importToIceberg(
            tableIdent, sourceIdent, partitionFilter, checkDuplicateFiles, parallelism));
  }

  private InternalRow[] toOutputRows(Snapshot snapshot) {
    Map<String, String> summary = snapshot.summary();
    return new InternalRow[] {
      newInternalRow(addedFilesCount(summary), changedPartitionCount(summary))
    };
  }

  private long addedFilesCount(Map<String, String> stats) {
    return PropertyUtil.propertyAsLong(stats, SnapshotSummary.ADDED_FILES_PROP, 0L);
  }

  private Long changedPartitionCount(Map<String, String> stats) {
    return PropertyUtil.propertyAsNullableLong(stats, SnapshotSummary.CHANGED_PARTITION_COUNT_PROP);
  }

  private boolean isFileIdentifier(Identifier ident) {
    String[] namespace = ident.namespace();
    return namespace.length == 1
        && (namespace[0].equalsIgnoreCase("orc")
            || namespace[0].equalsIgnoreCase("parquet")
            || namespace[0].equalsIgnoreCase("avro"));
  }

  private InternalRow[] importToIceberg(
      Identifier destIdent,
      Identifier sourceIdent,
      Map<String, String> partitionFilter,
      boolean checkDuplicateFiles,
      int parallelism) {
    return modifyIcebergTable(
        destIdent,
        table -> {
          ensureNameMappingPresent(table);

          if (isFileIdentifier(sourceIdent)) {
            Path sourcePath = new Path(sourceIdent.name());
            String format = sourceIdent.namespace()[0];
            importFileTable(
                table, sourcePath, format, partitionFilter, checkDuplicateFiles, parallelism);
          } else {
            importCatalogTable(
                table, sourceIdent, partitionFilter, checkDuplicateFiles, parallelism);
          }

          Snapshot snapshot = table.currentSnapshot();
          return toOutputRows(snapshot);
        });
  }

  private static void ensureNameMappingPresent(Table table) {
    if (table.properties().get(TableProperties.DEFAULT_NAME_MAPPING) == null) {
      // Forces Name based resolution instead of position based resolution
      NameMapping mapping = MappingUtil.create(table.schema());
      String mappingJson = NameMappingParser.toJson(mapping);
      table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson).commit();
    }
  }

  private void importFileTable(
      Table table,
      Path tableLocation,
      String format,
      Map<String, String> partitionFilter,
      boolean checkDuplicateFiles,
      int parallelism) {

    org.apache.spark.sql.execution.datasources.PartitionSpec inferredSpec =
        Spark3Util.getInferredSpec(spark(), tableLocation);

    List<String> sparkPartNames =
        JavaConverters.seqAsJavaList(inferredSpec.partitionColumns()).stream()
            .map(StructField::name)
            .collect(Collectors.toList());
    PartitionSpec compatibleSpec = SparkTableUtil.findCompatibleSpec(sparkPartNames, table);

    SparkTableUtil.validatePartitionFilter(compatibleSpec, partitionFilter, table.name());

    // List Partitions via Spark InMemory file search interface
    List<SparkPartition> partitions =
        Spark3Util.getPartitions(spark(), tableLocation, format, partitionFilter, compatibleSpec);

    if (table.spec().isUnpartitioned()) {
      Preconditions.checkArgument(
          partitions.isEmpty(), "Cannot add partitioned files to an unpartitioned table");
      Preconditions.checkArgument(
          partitionFilter.isEmpty(),
          "Cannot use a partition filter when importing" + "to an unpartitioned table");

      // Build a Global Partition for the source
      SparkPartition partition =
          new SparkPartition(Collections.emptyMap(), tableLocation.toString(), format);
      importPartitions(
          table, ImmutableList.of(partition), checkDuplicateFiles, compatibleSpec, parallelism);
    } else {
      Preconditions.checkArgument(
          !partitions.isEmpty(), "Cannot find any matching partitions in table %s", table.name());
      importPartitions(table, partitions, checkDuplicateFiles, compatibleSpec, parallelism);
    }
  }

  private void importCatalogTable(
      Table table,
      Identifier sourceIdent,
      Map<String, String> partitionFilter,
      boolean checkDuplicateFiles,
      int parallelism) {
    String stagingLocation = getMetadataLocation(table);
    TableIdentifier sourceTableIdentifier = Spark3Util.toV1TableIdentifier(sourceIdent);
    SparkTableUtil.importSparkTable(
        spark(),
        sourceTableIdentifier,
        table,
        stagingLocation,
        partitionFilter,
        checkDuplicateFiles,
        parallelism);
  }

  private void importPartitions(
      Table table,
      List<SparkTableUtil.SparkPartition> partitions,
      boolean checkDuplicateFiles,
      PartitionSpec spec,
      int parallelism) {
    String stagingLocation = getMetadataLocation(table);
    SparkTableUtil.importSparkPartitions(
        spark(), partitions, table, spec, stagingLocation, checkDuplicateFiles, parallelism);
  }

  private String getMetadataLocation(Table table) {
    String defaultValue = LocationUtil.stripTrailingSlash(table.location()) + "/metadata";
    return LocationUtil.stripTrailingSlash(
        table.properties().getOrDefault(TableProperties.WRITE_METADATA_LOCATION, defaultValue));
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String description() {
    return "AddFiles";
  }
}
