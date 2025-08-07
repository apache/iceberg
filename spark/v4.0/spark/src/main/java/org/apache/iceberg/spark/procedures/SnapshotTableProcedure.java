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
import java.util.Map;
import org.apache.iceberg.actions.SnapshotTable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.procedures.BoundProcedure;
import org.apache.spark.sql.connector.catalog.procedures.ProcedureParameter;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

class SnapshotTableProcedure extends BaseProcedure {

  static final String NAME = "snapshot";

  private static final ProcedureParameter SOURCE_TABLE_PARAM =
      requiredInParameter("source_table", DataTypes.StringType);
  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);
  private static final ProcedureParameter LOCATION_PARAM =
      optionalInParameter("location", DataTypes.StringType);
  private static final ProcedureParameter PROPERTIES_PARAM =
      optionalInParameter("properties", STRING_MAP);
  private static final ProcedureParameter PARALLELISM_PARAM =
      optionalInParameter("parallelism", DataTypes.IntegerType);

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        SOURCE_TABLE_PARAM, TABLE_PARAM, LOCATION_PARAM, PROPERTIES_PARAM, PARALLELISM_PARAM
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("imported_files_count", DataTypes.LongType, false, Metadata.empty())
          });

  private SnapshotTableProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  public static SparkProcedures.ProcedureBuilder builder() {
    return new BaseProcedure.Builder<SnapshotTableProcedure>() {
      @Override
      protected SnapshotTableProcedure doBuild() {
        return new SnapshotTableProcedure(tableCatalog());
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
    ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);

    String source = input.asString(SOURCE_TABLE_PARAM, null);
    Preconditions.checkArgument(
        source != null && !source.isEmpty(),
        "Cannot handle an empty identifier for argument source_table");

    String dest = input.asString(TABLE_PARAM, null);
    Preconditions.checkArgument(
        dest != null && !dest.isEmpty(), "Cannot handle an empty identifier for argument table");

    String snapshotLocation = input.asString(LOCATION_PARAM, null);

    Map<String, String> properties = input.asStringMap(PROPERTIES_PARAM, ImmutableMap.of());

    Preconditions.checkArgument(
        !source.equals(dest),
        "Cannot create a snapshot with the same name as the source of the snapshot.");
    SnapshotTable action = SparkActions.get().snapshotTable(source).as(dest);

    if (snapshotLocation != null) {
      action.tableLocation(snapshotLocation);
    }

    if (input.isProvided(PARALLELISM_PARAM)) {
      int parallelism = input.asInt(PARALLELISM_PARAM);
      Preconditions.checkArgument(parallelism > 0, "Parallelism should be larger than 0");
      action = action.executeWith(SparkTableUtil.migrationService(parallelism));
    }

    SnapshotTable.Result result = action.tableProperties(properties).execute();
    return asScanIterator(OUTPUT_TYPE, newInternalRow(result.importedDataFilesCount()));
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String description() {
    return "SnapshotTableProcedure";
  }
}
