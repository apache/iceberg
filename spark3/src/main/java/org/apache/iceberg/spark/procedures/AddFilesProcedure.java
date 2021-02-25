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

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.runtime.BoxedUnit;

public class AddFilesProcedure extends BaseProcedure {

  enum Format {
    hive,
    orc,
    parquet
  }

  private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[]{
      ProcedureParameter.required("table", DataTypes.StringType),
      ProcedureParameter.required("source_table", DataTypes.StringType),
      ProcedureParameter.optional("partition", STRING_MAP)
  };

  private static final StructType OUTPUT_TYPE = new StructType(new StructField[]{
      new StructField("added_files_count", DataTypes.LongType, false, Metadata.empty())
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
    Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());

    String sourceTable = args.getString(1);
    Format format = parseFormat(sourceTable);

    Map<String, String> partition = Maps.newHashMap();
    if (!args.isNullAt(2)) {
      args.getMap(2).foreach(DataTypes.StringType, DataTypes.StringType,
          (k, v) -> {
            partition.put(k.toString(), v.toString());
            return BoxedUnit.UNIT;
          });
    }

    Long filesAdded = importToIceberg(tableIdent, sourceTable, format, partition);
    return new InternalRow[]{newInternalRow(filesAdded)};
  }

  private Format parseFormat(String source) {
    String[] parts = source.split("\\.", 2);
    if (parts.length == 2) {
      if (parts[0].toLowerCase(Locale.ROOT).equals("orc")) {
        return Format.orc;
      }
      if (parts[0].toLowerCase(Locale.ROOT).equals("parquet")) {
        return Format.parquet;
      }
      return Format.hive;
    }
    return Format.hive;
  }

  private long importToIceberg(Identifier destIdent, String sourceIdent, Format format, Map<String, String> partition) {
    return modifyIcebergTable(destIdent, table -> {

      validatePartitionSpec(table, partition);
      applyNameMappingIfMissing(table);

      if (format != Format.hive) {
        String[] parts = sourceIdent.split("\\.", 2);
        Path sourcePath = new Path(parts[1]);

        Configuration conf = spark().sessionState().newHadoopConf();
        FileSystem fs;
        Boolean isFile;
        try {
          fs = sourcePath.getFileSystem(conf);
          isFile = fs.getFileStatus(sourcePath).isFile();
        } catch (IOException e) {
          throw new RuntimeException("Unable to access add_file path", e);
        }

        if (isFile) {
          importFile(table, sourcePath, format, partition);
        } else {
          importFileTable(table, sourcePath, format, partition);
        }
      } else {
        importHiveTable(table, sourceIdent, partition);
      }

      Snapshot snapshot = table.currentSnapshot();
      Long numAddedFiles = Long.parseLong(snapshot.summary().get(SnapshotSummary.ADDED_FILES_PROP));
      return numAddedFiles;
    });
  }

  private static void applyNameMappingIfMissing(Table table) {
    if (table.properties().get(TableProperties.DEFAULT_NAME_MAPPING) == null) {
      // Forces Name based resolution instead of position based resolution
      NameMapping mapping = MappingUtil.create(table.schema());
      String mappingJson = NameMappingParser.toJson(mapping);
      table.updateProperties()
          .set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson)
          .commit();
    }
  }

  private int importFile(Table table, Path file, Format format,  Map<String, String> partition) {
    if (partition.isEmpty() && !table.spec().isUnpartitioned()) {
      throw new IllegalArgumentException("Cannot add a file to a partitioned table without specifying the partition " +
          "which it should be placed in");
    }
    if (!partition.isEmpty() && table.spec().isUnpartitioned()) {
      throw new IllegalArgumentException("Cannot add a file to an unpartitioned table while specifying the partition " +
          "which it should be placed in");
    }

    PartitionSpec spec = table.spec();
    MetricsConfig metricsConfig = MetricsConfig.fromProperties(table.properties());
    String partitionURI = file.toString();
    Configuration conf = spark().sessionState().newHadoopConf();

    List<DataFile> files =
        SparkTableUtil.listPartition(partition, partitionURI, format.name(), spec, conf, metricsConfig);

    if (files.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("No file found for add_file command. Looking for a file at URI %s", partitionURI));
    }

    // Add Snapshot Summary Info?
    AppendFiles append = table.newAppend();
    files.forEach(append::appendFile);
    append.commit();
    return 1;
  }

  private void importFileTable(Table table, Path tableLocation, Format format, Map<String, String> partition) {
    // List Partitions via Spark InMemory file search interface
    List<SparkTableUtil.SparkPartition> partitions =
        filterPartitions(Spark3Util.getPartitions(spark(), tableLocation, format.name()), partition);

    importPartitions(table, partitions);
  }

  private void importHiveTable(Table table, String sourceTable, Map<String, String> partition) {
    // Read partitions Spark via Catalog Interface
    List<SparkTableUtil.SparkPartition> partitions =
        filterPartitions(SparkTableUtil.getPartitions(spark(), sourceTable), partition);

    importPartitions(table, partitions);
  }

  private void importPartitions(Table table, List<SparkTableUtil.SparkPartition> partitions) {
    String stagingLocation = table.properties()
        .getOrDefault(TableProperties.WRITE_METADATA_LOCATION, table.location() + "/metadata");
    SparkTableUtil.importSparkPartitions(spark(), partitions, table, table.spec(), stagingLocation);
  }

  private List<SparkTableUtil.SparkPartition> filterPartitions(List<SparkTableUtil.SparkPartition> partitions,
                                                               Map<String, String> partition) {
    if (partition.isEmpty()) {
      // No partition filter arg

      if (partitions.isEmpty()) {
        throw new IllegalArgumentException("Cannot add files, no files found in the table.");
      }
      return partitions;
    } else {
      // Partition filter arg passed

      List<SparkTableUtil.SparkPartition> filteredPartitions = partitions
          .stream().filter(p -> p.getValues().equals(partition)) // Todo Support Wildcards
          .collect(Collectors.toList());

      if (filteredPartitions.isEmpty()) {
        throw new IllegalArgumentException(
            String.format("No partitions found in table for add_file command. " +
                    "Looking for a partition with the value %s",
                partition.entrySet().stream().map(e -> e.getValue() + "=" + e.getValue())
                    .collect(Collectors.joining(",")))
        );
      }

      return filteredPartitions;
    }
  }

  @Override
  public String description() {
    return null;
  }

  private void validatePartitionSpec(Table table, Map<String, String> partition) {
    List<PartitionField> partitionFields = table.spec().fields();

    boolean tablePartitioned = !partitionFields.isEmpty();
    boolean partitionSpecPassed = !partition.isEmpty();

    if (tablePartitioned && partitionSpecPassed) {
      // Has Partitions, Check that they are valid
      if (partitionFields.size() != partition.size()) {
        throw new IllegalArgumentException(
          String.format(
            "Cannot add data files to target table %s because that table is partitioned, " +
            "but the number of columns in the provided partition spec (%d) " +
            "does not match the number of partitioned columns in table (%d)",
              table.name(), partition.size(), partitionFields.size()));
      }
      partitionFields.forEach(field -> {
        if (!partition.containsKey(field.name())) {
          throw new IllegalArgumentException(
            String.format(
              "Cannot add files to target table %s. %s is partitioned but the specified partition spec " +
              "refers to a column that is not partitioned: '%s'",
                table.name(), table.name(), field.name())
            );
        }
      });
    } else {
      if (partitionSpecPassed) {
        throw new IllegalArgumentException(
          String.format(
            "Cannot add files to target table %s which is not partitioned, but a partition spec was provided",
              table.name()));
      }
    }
  }
}
