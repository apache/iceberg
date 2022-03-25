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

import java.util.Arrays;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

class GenerateSymlinkFormatManifestProcedure extends BaseProcedure {
  private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[]{
      ProcedureParameter.required("table", DataTypes.StringType),
      ProcedureParameter.optional("symlink_root_location", DataTypes.StringType)
  };

  private static final StructType OUTPUT_TYPE = new StructType(new StructField[]{
      new StructField("snapshot_id", DataTypes.LongType, false, Metadata.empty()),
      new StructField("data_file_count", DataTypes.LongType, false, Metadata.empty())
  });

  private static final String PROCEDURE_CONTEXT = "generate symlink format manifest";
  private static final String SYMLINK_DIRECTORY_DEFAULT = "_symlink_format_manifest";

  private GenerateSymlinkFormatManifestProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  public static SparkProcedures.ProcedureBuilder builder() {
    return new Builder<GenerateSymlinkFormatManifestProcedure>() {
      @Override
      protected GenerateSymlinkFormatManifestProcedure doBuild() {
        return new GenerateSymlinkFormatManifestProcedure(tableCatalog());
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
    String tableIdent = args.getString(0);
    Preconditions.checkArgument(tableIdent != null && !tableIdent.isEmpty(),
        "Cannot handle an empty identifier for argument table");

    CatalogPlugin defaultCatalog = spark().sessionState().catalogManager().currentCatalog();
    Spark3Util.CatalogAndIdentifier catalogAndIdent = Spark3Util.catalogAndIdentifier(
        PROCEDURE_CONTEXT, spark(), tableIdent, defaultCatalog);
    SparkTable sparkTable = loadSparkTable(catalogAndIdent.identifier());
    org.apache.iceberg.Table icebergTable = sparkTable.table();
    ValidationException.check(icebergTable.currentSnapshot() != null,
        "Cannot generate symlink manifests for an empty table");

    long snapshotId = icebergTable.currentSnapshot().snapshotId();
    String symlinkRootLocation = args.isNullAt(1) ?
        defaultSymlinkManifestRootLocation(icebergTable.location(), snapshotId) : args.getString(1);

    Types.StructType partitionType = Partitioning.partitionType(icebergTable);
    Dataset<Row> entries = SparkTableUtil.loadCatalogMetadataTable(spark(), icebergTable, MetadataTableType.ENTRIES)
        .filter("status < 2 AND data_file.content = 0");

    long rowCount = entries.count();
    if (partitionType.fields().isEmpty()) {
      entries.select("data_file.file_path")
          .write()
          .format("parquet")
          .save(symlinkRootLocation);
    } else {
      String[] partitionColumns = partitionType.fields().stream()
          .map(Types.NestedField::name)
          .toArray(String[]::new);
      String[] entryPartitionColumns = Arrays.stream(partitionColumns)
          .map(name -> "data_file.partition." + name)
          .toArray(String[]::new);
      entries.select("data_file.file_path", entryPartitionColumns)
          .write()
          .partitionBy(partitionColumns)
          .format("parquet")
          .save(symlinkRootLocation);
    }

    return new InternalRow[] {newInternalRow(snapshotId, rowCount)};
  }

  @Override
  public String description() {
    return "GenerateSymlinkFormatManifestProcedure";
  }

  private String defaultSymlinkManifestRootLocation(String tableLocation, long snapshotId) {
    int len = tableLocation.length();
    StringBuilder sb = new StringBuilder();
    sb.append(tableLocation);
    if (sb.charAt(len - 1) != '/') {
      sb.append('/');
    }

    sb.append(SYMLINK_DIRECTORY_DEFAULT);
    sb.append('/');
    sb.append(snapshotId);
    return sb.toString();
  }
}
