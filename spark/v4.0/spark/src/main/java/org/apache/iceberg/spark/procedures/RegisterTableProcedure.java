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
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.iceberg.spark.source.HasIcebergCatalog;
import org.apache.iceberg.util.LocationUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.procedures.BoundProcedure;
import org.apache.spark.sql.connector.catalog.procedures.ProcedureParameter;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RegisterTableProcedure extends BaseProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(RegisterTableProcedure.class);

  static final String NAME = "register_table";
  private static final String METADATA_FOLDER_NAME = "metadata";
  private static final String METADATA_FILE_EXTENSION = ".metadata.json";
  private static final Pattern METADATA_VERSION_PATTERN =
      Pattern.compile(
          "(?<version>\\d+)-(?<uuid>[-a-fA-F0-9]*)(?<compression>\\.[a-zA-Z0-9]+)?"
              + Pattern.quote(METADATA_FILE_EXTENSION)
              + "(?<compression2>\\.[a-zA-Z0-9]+)?");
  private static final Pattern HADOOP_METADATA_VERSION_PATTERN =
      Pattern.compile(
          "v(?<version>\\d+)(?<compression>\\.[a-zA-Z0-9]+)?"
              + Pattern.quote(METADATA_FILE_EXTENSION)
              + "(?<compression2>\\.[a-zA-Z0-9]+)?");

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        requiredInParameter("table", DataTypes.StringType),
        requiredInParameter("metadata_file", DataTypes.StringType)
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("current_snapshot_id", DataTypes.LongType, true, Metadata.empty()),
            new StructField("total_records_count", DataTypes.LongType, true, Metadata.empty()),
            new StructField("total_data_files_count", DataTypes.LongType, true, Metadata.empty())
          });

  private RegisterTableProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<RegisterTableProcedure>() {
      @Override
      protected RegisterTableProcedure doBuild() {
        return new RegisterTableProcedure(tableCatalog());
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
    TableIdentifier tableName =
        Spark3Util.identifierToTableIdentifier(toIdentifier(args.getString(0), "table"));
    String metadataFile = args.getString(1);
    Preconditions.checkArgument(
        tableCatalog() instanceof HasIcebergCatalog,
        "Cannot use Register Table in a non-Iceberg catalog");
    Preconditions.checkArgument(
        metadataFile != null && !metadataFile.isEmpty(),
        "Cannot handle an empty argument metadata_file");

    String metadataLocation = LocationUtil.stripTrailingSlash(metadataFile);
    if (metadataLocation.endsWith(METADATA_FOLDER_NAME)) {
      Path metadataDirectory = new Path(metadataLocation);
      FileSystem fileSystem = getFileSystem(metadataDirectory);
      metadataFile = resolveLatestMetadataLocation(fileSystem, metadataDirectory);
    }

    Catalog icebergCatalog = ((HasIcebergCatalog) tableCatalog()).icebergCatalog();
    Table table = icebergCatalog.registerTable(tableName, metadataFile);
    Long currentSnapshotId = null;
    Long totalDataFiles = null;
    Long totalRecords = null;

    Snapshot currentSnapshot = table.currentSnapshot();
    if (currentSnapshot != null) {
      currentSnapshotId = currentSnapshot.snapshotId();
      totalDataFiles =
          Long.parseLong(currentSnapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
      totalRecords =
          Long.parseLong(currentSnapshot.summary().get(SnapshotSummary.TOTAL_RECORDS_PROP));
    }

    return asScanIterator(
        OUTPUT_TYPE, newInternalRow(currentSnapshotId, totalRecords, totalDataFiles));
  }

  private FileSystem getFileSystem(Path path) {
    try {
      Configuration sparkHadoopConf = spark().sessionState().newHadoopConf();
      return path.getFileSystem(sparkHadoopConf);
    } catch (IOException e) {
      throw new RuntimeException("Failed to get FileSystem for path: " + path, e);
    }
  }

  public static String resolveLatestMetadataLocation(FileSystem fileSystem, Path metadataPath) {
    int maxVersion = -1;
    long lastModifiedTime = -1;
    Path metadataFile = null;
    boolean duplicateVersions = false;

    try {
      FileStatus[] files =
          fileSystem.listStatus(
              metadataPath, name -> name.getName().contains(METADATA_FILE_EXTENSION));
      for (FileStatus file : files) {
        int version = parseMetadataVersionFromFileName(file.getPath().getName());
        if (version > maxVersion) {
          maxVersion = version;
          metadataFile = file.getPath();
          lastModifiedTime = file.getModificationTime();
          duplicateVersions = false;
        } else if (version == maxVersion) {
          duplicateVersions = true;

          long modifiedTime = file.getModificationTime();
          if (modifiedTime > lastModifiedTime) {
            lastModifiedTime = modifiedTime;
            metadataFile = file.getPath();
          }
        }
      }
    } catch (IOException io) {
      throw new NotFoundException("Unable to find metadata at location %s", metadataPath);
    }

    if (duplicateVersions) {
      LOG.info(
          "Multiple metadata files of most recent version {} found at location {}. "
              + "Using most recently modified file: {}",
          maxVersion,
          metadataPath,
          metadataFile);
    }
    if (metadataFile == null) {
      throw new NotFoundException("No metadata found at location %s", metadataPath);
    }

    return metadataFile.toString();
  }

  static int parseMetadataVersionFromFileName(String fileName) {
    Matcher matcher = METADATA_VERSION_PATTERN.matcher(fileName);
    if (matcher.matches()) {
      return Integer.parseInt(matcher.group("version"));
    }
    matcher = HADOOP_METADATA_VERSION_PATTERN.matcher(fileName);
    if (matcher.matches()) {
      return Integer.parseInt(matcher.group("version"));
    }
    return -1;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String description() {
    return "RegisterTableProcedure";
  }
}
