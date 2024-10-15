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

import java.util.Optional;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.BaseCatalog;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.iceberg.util.WapUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * A procedure that applies changes in a snapshot created within a Write-Audit-Publish workflow with
 * a wap_id and creates a new snapshot which will be set as the current snapshot in a table.
 *
 * <p><em>Note:</em> this procedure invalidates all cached Spark plans that reference the affected
 * table.
 *
 * @see org.apache.iceberg.ManageSnapshots#cherrypick(long)
 */
class PublishChangesProcedure extends BaseProcedure {

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("table", DataTypes.StringType),
        ProcedureParameter.required("wap_id", DataTypes.StringType)
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("source_snapshot_id", DataTypes.LongType, false, Metadata.empty()),
            new StructField("current_snapshot_id", DataTypes.LongType, false, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new Builder<PublishChangesProcedure>() {
      @Override
      protected PublishChangesProcedure doBuild() {
        return new PublishChangesProcedure(catalog());
      }
    };
  }

  private PublishChangesProcedure(BaseCatalog catalog) {
    super(catalog);
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
    String wapId = args.getString(1);

    return modifyIcebergTable(
        tableIdent,
        table -> {
          Optional<Snapshot> wapSnapshot =
              Optional.ofNullable(
                  Iterables.find(
                      table.snapshots(),
                      snapshot -> wapId.equals(WapUtil.stagedWapId(snapshot)),
                      null));
          if (!wapSnapshot.isPresent()) {
            throw new ValidationException(String.format("Cannot apply unknown WAP ID '%s'", wapId));
          }

          long wapSnapshotId = wapSnapshot.get().snapshotId();
          table.manageSnapshots().cherrypick(wapSnapshotId).commit();

          Snapshot currentSnapshot = table.currentSnapshot();

          InternalRow outputRow = newInternalRow(wapSnapshotId, currentSnapshot.snapshotId());
          return new InternalRow[] {outputRow};
        });
  }

  @Override
  public String description() {
    return "ApplyWapChangesProcedure";
  }
}
