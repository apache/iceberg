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

import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A procedure that removes corrupt snapshots whose ManifestList files are missing. To expire
 * healthy snapshots, use ExpireSnapshotsProcedure.
 */
public class RemoveSnapshotsProcedure extends BaseProcedure {

  private static final Logger LOG = LoggerFactory.getLogger(RemoveSnapshotsProcedure.class);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("table", DataTypes.StringType),
        ProcedureParameter.required("snapshot_ids", DataTypes.createArrayType(DataTypes.LongType))
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("deleted_snapshots_count", DataTypes.LongType, true, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<RemoveSnapshotsProcedure>() {
      @Override
      protected RemoveSnapshotsProcedure doBuild() {
        return new RemoveSnapshotsProcedure(tableCatalog());
      }
    };
  }

  private RemoveSnapshotsProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
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
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public InternalRow[] call(InternalRow args) {
    Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
    long[] snapshotIds = args.getArray(1).toLongArray();

    return modifyIcebergTable(
        tableIdent,
        table -> {
          ExpireSnapshots expireSnapshots = table.expireSnapshots();

          expireSnapshots.expireOlderThan(0L);
          if (snapshotIds != null) {
            for (long snapshotId : snapshotIds) {
              expireSnapshots.expireSnapshotId(snapshotId);
            }
            expireSnapshots.cleanExpiredFiles(false).commit();
            return toOutputRows(snapshotIds.length);
          } else {
            return toOutputRows(0);
          }
        });
  }

  private InternalRow[] toOutputRows(long deletedSnapshotsCount) {
    InternalRow row = newInternalRow(deletedSnapshotsCount);

    return new InternalRow[] {row};
  }

  @Override
  public String description() {
    return "RemoveSnapshotsProcedure";
  }
}
