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

import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrderStatsHandler;
import org.apache.iceberg.SortOrderStatsHandler.PartitionOverlapStats;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.procedures.BoundProcedure;
import org.apache.spark.sql.connector.catalog.procedures.ProcedureParameter;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A procedure that computes per-partition file overlap statistics on the table's sort order,
 * reading only data file metadata (column bounds) — no data files are opened and nothing is
 * committed to the table.
 *
 * <p>The maximum overlap depth of a partition is the largest number of data files whose sort-key
 * ranges cover a single point. A value of 1 means the partition is perfectly clustered on the first
 * sort field; values close to the file count mean sorting has not materialized in the file layout.
 *
 * @see SortOrderStatsHandler
 */
public class ComputeSortOrderStatsProcedure extends BaseProcedure {

  static final String NAME = "compute_sort_order_stats";

  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter SNAPSHOT_ID_PARAM =
      optionalInParameter("snapshot_id", DataTypes.LongType);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {TABLE_PARAM, SNAPSHOT_ID_PARAM};

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("partition", DataTypes.StringType, true, Metadata.empty()),
            new StructField("spec_id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("file_count", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("files_missing_bounds", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("max_overlap_depth", DataTypes.IntegerType, true, Metadata.empty()),
            new StructField("avg_overlap_depth", DataTypes.DoubleType, true, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new Builder<ComputeSortOrderStatsProcedure>() {
      @Override
      protected ComputeSortOrderStatsProcedure doBuild() {
        return new ComputeSortOrderStatsProcedure(tableCatalog());
      }
    };
  }

  private ComputeSortOrderStatsProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
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
    ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);
    Identifier tableIdent = input.ident(TABLE_PARAM);
    Long snapshotId = input.asLong(SNAPSHOT_ID_PARAM, null);

    return withIcebergTable(
        tableIdent,
        table -> {
          List<PartitionOverlapStats> stats = SortOrderStatsHandler.computeStats(table, snapshotId);
          InternalRow[] rows = new InternalRow[stats.size()];
          for (int i = 0; i < stats.size(); i++) {
            PartitionOverlapStats stat = stats.get(i);
            PartitionSpec spec = table.specs().get(stat.specId());
            String partitionPath =
                spec.isPartitioned() ? spec.partitionToPath(stat.partition()) : null;
            rows[i] =
                newInternalRow(
                    partitionPath == null ? null : UTF8String.fromString(partitionPath),
                    stat.specId(),
                    stat.fileCount(),
                    stat.filesMissingBounds(),
                    stat.maxOverlapDepth(),
                    stat.avgOverlapDepth());
          }

          return asScanIterator(OUTPUT_TYPE, rows);
        });
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String description() {
    return "ComputeSortOrderStatsProcedure";
  }
}
