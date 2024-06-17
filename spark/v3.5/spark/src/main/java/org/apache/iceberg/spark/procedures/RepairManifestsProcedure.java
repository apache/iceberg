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
import org.apache.iceberg.actions.RepairManifests;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.actions.RepairManifestsSparkAction;
import org.apache.iceberg.spark.actions.RewriteManifestsSparkAction;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * A procedure that repairs manifests with one or both of the following: duplicate files committed
 * to the table, files referenced by the table that do not exist on disk. Care should be taken with
 * the latter to investigate the underlying cause and recover files if possible. The
 * remove_missing_files procedure exists for emergency purposes only.
 *
 * <p><em>Note:</em> this procedure invalidates all cached Spark plans that reference the affected
 * table.
 *
 * @see SparkActions#repairManifests(Table) ()
 */
class RepairManifestsProcedure extends BaseProcedure {

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("table", DataTypes.StringType),
        ProcedureParameter.optional("use_caching", DataTypes.BooleanType),
        ProcedureParameter.optional("dry_run", DataTypes.BooleanType),
        ProcedureParameter.optional("remove_duplicate_files", DataTypes.BooleanType),
        ProcedureParameter.optional("remove_missing_files", DataTypes.BooleanType)
      };

  // counts are not nullable since the action result is never null
  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField(
                "rewritten_manifests_count", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField(
                "added_manifests_count", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField(
                "duplicate_files_removed_count", DataTypes.LongType, false, Metadata.empty()),
            new StructField(
                "missing_files_removed_count", DataTypes.LongType, false, Metadata.empty())
          });

  public static SparkProcedures.ProcedureBuilder builder() {
    return new BaseProcedure.Builder<RepairManifestsProcedure>() {
      @Override
      protected RepairManifestsProcedure doBuild() {
        return new RepairManifestsProcedure(tableCatalog());
      }
    };
  }

  private RepairManifestsProcedure(TableCatalog tableCatalog) {
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
  public InternalRow[] call(InternalRow args) {
    Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
    Boolean useCaching = args.isNullAt(1) ? null : args.getBoolean(1);
    Boolean dryRun = args.isNullAt(2) ? null : args.getBoolean(2);
    Boolean removeDuplicates = args.isNullAt(3) ? null : args.getBoolean(3);
    Boolean removeMissingFiles = args.isNullAt(4) ? null : args.getBoolean(4);

    return modifyIcebergTable(
        tableIdent,
        table -> {
          RepairManifestsSparkAction action = actions().repairManifests(table);

          if (useCaching != null) {
            action.option(RewriteManifestsSparkAction.USE_CACHING, useCaching.toString());
          }

          if (dryRun != null) {
            action.dryRun(dryRun);
          }
          if (removeDuplicates != null && removeDuplicates) {
            action.removeDuplicateCommittedFiles();
          }
          if (removeMissingFiles != null && removeMissingFiles) {
            action.removeMissingFiles();
          }

          RepairManifests.Result result = action.execute();

          return toOutputRows(result);
        });
  }

  private InternalRow[] toOutputRows(RepairManifests.Result result) {
    int rewrittenManifestsCount = Iterables.size(result.rewrittenManifests());
    int addedManifestsCount = Iterables.size(result.addedManifests());
    InternalRow row =
        newInternalRow(
            rewrittenManifestsCount,
            addedManifestsCount,
            result.duplicateFilesRemoved(),
            result.duplicateFilesRemoved());
    return new InternalRow[] {row};
  }

  @Override
  public String description() {
    return "RepairManifestsProcedure";
  }
}
