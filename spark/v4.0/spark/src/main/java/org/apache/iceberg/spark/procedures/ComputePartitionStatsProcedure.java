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
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ComputePartitionStats;
import org.apache.iceberg.spark.actions.SparkActions;
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
 * A procedure that computes the stats incrementally from the last snapshot that has partition stats
 * file until the given snapshot (uses current snapshot if not specified) and writes the combined
 * result into a {@link PartitionStatisticsFile} after merging the partition stats. Does a full
 * compute if previous statistics file does not exist. Also registers the {@link
 * PartitionStatisticsFile} to table metadata.
 *
 * @see SparkActions#computePartitionStats(Table)
 */
public class ComputePartitionStatsProcedure extends BaseProcedure {

  static final String NAME = "compute_partition_stats";

  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter SNAPSHOT_ID_PARAM =
      optionalInParameter("snapshot_id", DataTypes.LongType);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {TABLE_PARAM, SNAPSHOT_ID_PARAM};

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField(
                "partition_statistics_file", DataTypes.StringType, true, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new Builder<ComputePartitionStatsProcedure>() {
      @Override
      protected ComputePartitionStatsProcedure doBuild() {
        return new ComputePartitionStatsProcedure(tableCatalog());
      }
    };
  }

  private ComputePartitionStatsProcedure(TableCatalog tableCatalog) {
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

    return modifyIcebergTable(
        tableIdent,
        table -> {
          ComputePartitionStats action = actions().computePartitionStats(table);
          if (snapshotId != null) {
            action.snapshot(snapshotId);
          }

          return asScanIterator(OUTPUT_TYPE, toOutputRows(action.execute()));
        });
  }

  private InternalRow[] toOutputRows(ComputePartitionStats.Result result) {
    PartitionStatisticsFile statisticsFile = result.statisticsFile();
    if (statisticsFile != null) {
      InternalRow row = newInternalRow(UTF8String.fromString(statisticsFile.path()));
      return new InternalRow[] {row};
    } else {
      return new InternalRow[0];
    }
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String description() {
    return "ComputePartitionStatsProcedure";
  }
}
