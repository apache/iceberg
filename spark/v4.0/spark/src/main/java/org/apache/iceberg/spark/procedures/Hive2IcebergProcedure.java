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
import org.apache.iceberg.actions.Hive2Iceberg;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
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
import org.apache.spark.unsafe.types.UTF8String;

/** Hive2IcebergProcedure is responsible for migrating a regular Hive table to an Iceberg table. */
public class Hive2IcebergProcedure extends BaseProcedure {

  static final String NAME = "hive_to_iceberg";

  private static final ProcedureParameter SOURCE_TABLE_PARAM =
      requiredInParameter("source_table", DataTypes.StringType);
  private static final ProcedureParameter PARALLELISM_PARAM =
      optionalInParameter("parallelism", DataTypes.IntegerType);

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("successful", DataTypes.BooleanType, true, Metadata.empty()),
            new StructField("failure_message", DataTypes.StringType, true, Metadata.empty()),
            new StructField("latest_version", DataTypes.StringType, true, Metadata.empty())
          });

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {SOURCE_TABLE_PARAM, PARALLELISM_PARAM};

  private Hive2IcebergProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  public static SparkProcedures.ProcedureBuilder builder() {
    return new BaseProcedure.Builder<Hive2IcebergProcedure>() {
      @Override
      protected Hive2IcebergProcedure doBuild() {
        return new Hive2IcebergProcedure(tableCatalog());
      }
    };
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
    Hive2Iceberg action = SparkActions.get().hive2Iceberg(source);

    if (input.isProvided(PARALLELISM_PARAM)) {
      int parallelism = input.asInt(PARALLELISM_PARAM);
      Preconditions.checkArgument(parallelism > 0, "Parallelism should be larger than 0");
      action = action.parallelism(parallelism);
    }

    Hive2Iceberg.Result result = action.execute();
    InternalRow outputRow =
        newInternalRow(
            result.successful(),
            UTF8String.fromString(result.failureMessage()),
            UTF8String.fromString(result.latestVersion()));
    return asScanIterator(OUTPUT_TYPE, outputRow);
  }

  @Override
  public BoundProcedure bind(StructType inputType) {
    return this;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String description() {
    return "Hive2IcebergProcedure";
  }
}
