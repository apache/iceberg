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

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.iceberg.spark.source.HasIcebergCatalog;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

class RegisterTableProcedure extends BaseProcedure {
  private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[]{
      ProcedureParameter.required("table", DataTypes.StringType),
      ProcedureParameter.required("metadata_file", DataTypes.StringType)
  };

  private static final StructType OUTPUT_TYPE = new StructType(new StructField[]{
      new StructField("Current Snapshot", DataTypes.LongType, false, Metadata.empty()),
      new StructField("Manifests Registered", DataTypes.LongType, false, Metadata.empty()),
      new StructField("Datafiles Registered", DataTypes.LongType, false, Metadata.empty())
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
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public StructType outputType() {
    return OUTPUT_TYPE;
  }

  @Override
  public InternalRow[] call(InternalRow args) {
    TableIdentifier tableName = Spark3Util.identifierToTableIdentifier(toIdentifier(args.getString(0), "table"));
    String metadataFile = args.getString(1);
    Preconditions.checkArgument(metadataFile != null && !metadataFile.isEmpty(),
        "Cannot handle an empty argument metadata_file");

    Catalog icebergCatalog = ((HasIcebergCatalog) tableCatalog()).icebergCatalog();
    Table registeredTable = icebergCatalog.registerTable(tableName, metadataFile);
    long currentSnapshotId = registeredTable.currentSnapshot().snapshotId();
    long totalManifests = spark().table(tableCatalog().name() + "." + tableName + ".manifests").count();
    long totalDataFiles = spark().table(tableCatalog().name() + "." + tableName + ".files").count();

    return new InternalRow[] {newInternalRow(currentSnapshotId, totalManifests, totalDataFiles)};
  }

  @Override
  public String description() {
    return "MigrateTableProcedure";
  }
}

