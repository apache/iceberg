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

import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.actions.DropPartitionFromRefs;
import org.apache.iceberg.actions.DropPartitionFromRefs.RefType;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A procedure that removes all data files matching a partition filter from tags and/or branches.
 *
 * <p>Usage:
 *
 * <pre>
 * CALL catalog.system.drop_partition_from_refs(
 *     table  => 'db.tbl',
 *     where  => 'date = ''2024-01-01''',
 *     refs   => 'tags',         -- optional: 'tags' | 'branches' | 'all', default 'tags'
 *     dry_run => false          -- optional, default false
 * )
 * </pre>
 *
 * <p>Returns one row per updated ref with columns: {@code ref_name}, {@code previous_snapshot_id},
 * {@code new_snapshot_id}.
 */
public class DropPartitionFromRefsProcedure extends BaseProcedure {

  private static final ProcedureParameter TABLE_PARAM =
      ProcedureParameter.required("table", DataTypes.StringType);
  private static final ProcedureParameter WHERE_PARAM =
      ProcedureParameter.required("where", DataTypes.StringType);
  private static final ProcedureParameter REFS_PARAM =
      ProcedureParameter.optional("refs", DataTypes.StringType);
  private static final ProcedureParameter DRY_RUN_PARAM =
      ProcedureParameter.optional("dry_run", DataTypes.BooleanType);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {TABLE_PARAM, WHERE_PARAM, REFS_PARAM, DRY_RUN_PARAM};

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("ref_name", DataTypes.StringType, false, Metadata.empty()),
            new StructField("previous_snapshot_id", DataTypes.LongType, false, Metadata.empty()),
            new StructField("new_snapshot_id", DataTypes.LongType, false, Metadata.empty()),
          });

  public static SparkProcedures.ProcedureBuilder builder() {
    return new Builder<DropPartitionFromRefsProcedure>() {
      @Override
      protected DropPartitionFromRefsProcedure doBuild() {
        return new DropPartitionFromRefsProcedure(tableCatalog());
      }
    };
  }

  private DropPartitionFromRefsProcedure(TableCatalog tableCatalog) {
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
    ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);

    Identifier tableIdent = input.ident(TABLE_PARAM);
    String where = input.asString(WHERE_PARAM);
    String refTypeStr = input.asString(REFS_PARAM, "TAGS").toUpperCase(Locale.ROOT);
    boolean dryRun = input.asBoolean(DRY_RUN_PARAM, false);

    RefType refType;
    try {
      refType = RefType.valueOf(refTypeStr);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid refs value '" + refTypeStr + "'. Expected one of: TAGS, BRANCHES, ALL");
    }

    return modifyIcebergTable(
        tableIdent,
        table -> {
          Expression filter = filterExpression(tableIdent, where);

          // capture previous snapshot IDs before the action mutates the refs
          Map<String, Long> previousSnapshotIds = Maps.newHashMap();
          table.refs().forEach((name, ref) -> previousSnapshotIds.put(name, ref.snapshotId()));

          DropPartitionFromRefs.Result result =
              SparkActions.get(spark())
                  .dropPartitionFromRefs(table)
                  .filter(filter)
                  .refType(refType)
                  .dryRun(dryRun)
                  .execute();

          return result.updatedRefs().entrySet().stream()
              .map(
                  e ->
                      newInternalRow(
                          UTF8String.fromString(e.getKey()),
                          previousSnapshotIds.getOrDefault(e.getKey(), -1L),
                          e.getValue()))
              .toArray(InternalRow[]::new);
        });
  }

  @Override
  public String description() {
    return "DropPartitionFromRefsProcedure";
  }
}
