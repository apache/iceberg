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

  private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[]{
      ProcedureParameter.required("table", DataTypes.StringType),
      ProcedureParameter.required("path", DataTypes.StringType),
      ProcedureParameter.required("format", DataTypes.StringType),
      ProcedureParameter.optional("partition_value", STRING_MAP)
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
    String path = args.getString(1);
    String format =  args.getString(2);

    Map<String, String> partitionSpec = Maps.newHashMap();
    if (!args.isNullAt(3)) {
      args.getMap(3).foreach(DataTypes.StringType, DataTypes.StringType,
          (k, v) -> {
            partitionSpec.put(k.toString(), v.toString());
            return BoxedUnit.UNIT;
          });
    }

    Long filesAdded = importDataToIcebergTable(tableIdent, path, format, partitionSpec);

    return new InternalRow[]{newInternalRow(filesAdded)};
  }

  private Long importDataToIcebergTable(Identifier tableIdent, String path, String format,
                                        Map<String, String> partition) {
    return modifyIcebergTable(tableIdent, table -> {
      Configuration conf = spark().sessionState().newHadoopConf();
      Path dataPath = new Path(path);
      FileSystem fs;
      Boolean isFile;

      try {
        fs = dataPath.getFileSystem(conf);
        isFile = fs.getFileStatus(dataPath).isFile();
      } catch (IOException e) {
        throw new RuntimeException("Unable to access add_file path", e);
      }
      validatePartitionSpec(table, dataPath, fs, partition);

      if (table.properties().get(TableProperties.DEFAULT_NAME_MAPPING) == null) {
        // Forces Name based resolution instead of position based resolution
        NameMapping mapping = MappingUtil.create(table.schema());
        String mappingJson = NameMappingParser.toJson(mapping);
        table.updateProperties()
            .set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson)
            .commit();
      }

      if (isFile || !partition.isEmpty()) {
        // we do a list operation on the driver to import 1 file or 1 partition
        PartitionSpec spec = table.spec();
        MetricsConfig metricsConfig = MetricsConfig.fromProperties(table.properties());
        String partitionURI;

        if (isFile) {
          partitionURI = dataPath.toString();
        } else {
          partitionURI = dataPath.toString() + toPartitionPath(partition);
        }

        List<DataFile> files = SparkTableUtil.listPartition(partition, partitionURI, format, spec, conf, metricsConfig);
        if (files.size() == 0) {
          throw new IllegalArgumentException(String.format("No files found for add_file command. Looking in URI %s",
              partitionURI));
        }

        AppendFiles append = table.newAppend();
        files.forEach(append::appendFile);
        append.commit();
      } else {
        // Importing multiple partitions
        List<SparkTableUtil.SparkPartition> partitions = Spark3Util.getPartitions(spark(), dataPath, format);
        String stagingLocation = table.properties()
            .getOrDefault(TableProperties.WRITE_METADATA_LOCATION, table.location() + "/metadata");
        SparkTableUtil.importSparkPartitions(spark(), partitions, table, table.spec(), stagingLocation);
      }

      Snapshot snapshot = table.currentSnapshot();
      Long numAddedFiles = Long.parseLong(snapshot.summary().get(SnapshotSummary.ADDED_FILES_PROP));
      return numAddedFiles;
    });
  }

  @Override
  public String description() {
    return null;
  }

  private String toPartitionPath(Map<String, String> partition) {
    return partition.entrySet().stream()
        .map(entry -> entry.getKey() + "=" + entry.getValue())
        .collect(Collectors.joining("/", "/", ""));
  }

  private void validatePartitionSpec(Table table, Path path, FileSystem fs, Map<String, String> partition) {

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
    } else if (tablePartitioned && !partitionSpecPassed) {
      try {
        if (!fs.getFileStatus(path).isDirectory()) {
          throw new IllegalArgumentException(
            String.format(
              "Cannot add files to target table %s which is partitioned, but no partition spec was provided " +
              "and path '%s' is not a directory",
                table.name(), path));
        }
      } catch (IOException e) {
        throw new RuntimeException("Could not access path during add_files", e);
      }
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
