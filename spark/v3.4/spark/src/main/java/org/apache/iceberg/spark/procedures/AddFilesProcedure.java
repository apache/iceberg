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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionField;
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
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

class AddFilesProcedure extends BaseProcedure {

  private static final ProcedureParameter TABLE_PARAM =
      ProcedureParameter.required("table", DataTypes.StringType);
  private static final ProcedureParameter SOURCE_TABLE_PARAM =
      ProcedureParameter.required("source_table", DataTypes.StringType);
  private static final ProcedureParameter PARTITION_FILTER_PARAM =
      ProcedureParameter.optional("partition_filter", STRING_MAP);
  private static final ProcedureParameter CHECK_DUPLICATE_FILES_PARAM =
      ProcedureParameter.optional("check_duplicate_files", DataTypes.BooleanType);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        TABLE_PARAM, SOURCE_TABLE_PARAM, PARTITION_FILTER_PARAM, CHECK_DUPLICATE_FILES_PARAM
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("added_files_count", DataTypes.LongType, false, Metadata.empty()),
            new StructField("changed_partition_count", DataTypes.LongType, false, Metadata.empty()),
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
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public StructType outputType() {
    return OUTPUT_TYPE;
  }

  @Override
  public InternalRow[] call(InternalRow args) {
    ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);

    Identifier tableIdent = input.ident(TABLE_PARAM);

    CatalogPlugin sessionCat = spark().sessionState().catalogManager().v2SessionCatalog();
    Identifier sourceIdent = input.ident(SOURCE_TABLE_PARAM, sessionCat);

    Map<String, String> partitionFilter =
        input.asStringMap(PARTITION_FILTER_PARAM, ImmutableMap.of());

    boolean checkDuplicateFiles = input.asBoolean(CHECK_DUPLICATE_FILES_PARAM, true);

    return importToIceberg(tableIdent, sourceIdent, partitionFilter, checkDuplicateFiles);
  }

  private InternalRow[] toOutputRows(Snapshot snapshot) {
    Map<String, String> summary = snapshot.summary();
    return new InternalRow[] {
      newInternalRow(
          Long.parseLong(summary.getOrDefault(SnapshotSummary.ADDED_FILES_PROP, "0")),
          Long.parseLong(summary.getOrDefault(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP, "0")))
    };
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
      boolean checkDuplicateFiles) {
    return modifyIcebergTable(
        destIdent,
        table -> {
          validatePartitionSpec(table, partitionFilter);
          ensureNameMappingPresent(table);

          if (isFileIdentifier(sourceIdent)) {
            Path sourcePath = new Path(sourceIdent.name());
            String format = sourceIdent.namespace()[0];
            importFileTable(
                table, sourcePath, format, partitionFilter, checkDuplicateFiles, table.spec());
          } else {
            importCatalogTable(table, sourceIdent, partitionFilter, checkDuplicateFiles);
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
      PartitionSpec spec) {
    // List Partitions via Spark InMemory file search interface
    List<SparkPartition> partitions =
        Spark3Util.getPartitions(spark(), tableLocation, format, partitionFilter, spec);

    if (table.spec().isUnpartitioned()) {
      Preconditions.checkArgument(
          partitions.isEmpty(), "Cannot add partitioned files to an unpartitioned table");
      Preconditions.checkArgument(
          partitionFilter.isEmpty(),
          "Cannot use a partition filter when importing" + "to an unpartitioned table");

      // Build a Global Partition for the source
      SparkPartition partition =
          new SparkPartition(Collections.emptyMap(), tableLocation.toString(), format);
      importPartitions(table, ImmutableList.of(partition), checkDuplicateFiles);
    } else {
      Preconditions.checkArgument(
          !partitions.isEmpty(), "Cannot find any matching partitions in table %s", partitions);
      importPartitions(table, partitions, checkDuplicateFiles);
    }
  }

  private void importCatalogTable(
      Table table,
      Identifier sourceIdent,
      Map<String, String> partitionFilter,
      boolean checkDuplicateFiles) {
    String stagingLocation = getMetadataLocation(table);
    TableIdentifier sourceTableIdentifier = Spark3Util.toV1TableIdentifier(sourceIdent);
    SparkTableUtil.importSparkTable(
        spark(),
        sourceTableIdentifier,
        table,
        stagingLocation,
        partitionFilter,
        checkDuplicateFiles);
  }

  private void importPartitions(
      Table table, List<SparkTableUtil.SparkPartition> partitions, boolean checkDuplicateFiles) {
    String stagingLocation = getMetadataLocation(table);
    SparkTableUtil.importSparkPartitions(
        spark(), partitions, table, table.spec(), stagingLocation, checkDuplicateFiles);
  }

  private String getMetadataLocation(Table table) {
    String defaultValue = LocationUtil.stripTrailingSlash(table.location()) + "/metadata";
    return LocationUtil.stripTrailingSlash(
        table.properties().getOrDefault(TableProperties.WRITE_METADATA_LOCATION, defaultValue));
  }

  @Override
  public String description() {
    return "AddFiles";
  }

  private void validatePartitionSpec(Table table, Map<String, String> partitionFilter) {
    List<PartitionField> partitionFields = table.spec().fields();
    Set<String> partitionNames =
        table.spec().fields().stream().map(PartitionField::name).collect(Collectors.toSet());

    boolean tablePartitioned = !partitionFields.isEmpty();
    boolean partitionSpecPassed = !partitionFilter.isEmpty();

    // Check for any non-identity partition columns
    List<PartitionField> nonIdentityFields =
        partitionFields.stream()
            .filter(x -> !x.transform().isIdentity())
            .collect(Collectors.toList());
    Preconditions.checkArgument(
        nonIdentityFields.isEmpty(),
        "Cannot add data files to target table %s because that table is partitioned and contains non-identity"
            + "partition transforms which will not be compatible. Found non-identity fields %s",
        table.name(),
        nonIdentityFields);

    if (tablePartitioned && partitionSpecPassed) {
      // Check to see there are sufficient partition columns to satisfy the filter
      Preconditions.checkArgument(
          partitionFields.size() >= partitionFilter.size(),
          "Cannot add data files to target table %s because that table is partitioned, "
              + "but the number of columns in the provided partition filter (%s) "
              + "is greater than the number of partitioned columns in table (%s)",
          table.name(),
          partitionFilter.size(),
          partitionFields.size());

      // Check for any filters of non existent columns
      List<String> unMatchedFilters =
          partitionFilter.keySet().stream()
              .filter(filterName -> !partitionNames.contains(filterName))
              .collect(Collectors.toList());
      Preconditions.checkArgument(
          unMatchedFilters.isEmpty(),
          "Cannot add files to target table %s. %s is partitioned but the specified partition filter "
              + "refers to columns that are not partitioned: '%s' . Valid partition columns %s",
          table.name(),
          table.name(),
          unMatchedFilters,
          String.join(",", partitionNames));
    } else {
      Preconditions.checkArgument(
          !partitionSpecPassed,
          "Cannot use partition filter with an unpartitioned table %s",
          table.name());
    }
  }
}
