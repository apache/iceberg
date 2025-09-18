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

public class FastForwardBranchProcedure extends BaseProcedure {

  static final String NAME = "fast_forward";

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        requiredInParameter("table", DataTypes.StringType),
        requiredInParameter("branch", DataTypes.StringType),
        requiredInParameter("to", DataTypes.StringType)
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("branch_updated", DataTypes.StringType, false, Metadata.empty()),
            new StructField("previous_ref", DataTypes.LongType, true, Metadata.empty()),
            new StructField("updated_ref", DataTypes.LongType, false, Metadata.empty())
          });

  public static SparkProcedures.ProcedureBuilder builder() {
    return new Builder<FastForwardBranchProcedure>() {
      @Override
      protected FastForwardBranchProcedure doBuild() {
        return new FastForwardBranchProcedure(tableCatalog());
      }
    };
  }

  private FastForwardBranchProcedure(TableCatalog tableCatalog) {
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
    Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
    String from = args.getString(1);
    String to = args.getString(2);

    return modifyIcebergTable(
        tableIdent,
        table -> {
          Long snapshotBefore =
              table.snapshot(from) != null ? table.snapshot(from).snapshotId() : null;
          table.manageSnapshots().fastForwardBranch(from, to).commit();
          long snapshotAfter = table.snapshot(from).snapshotId();
          InternalRow outputRow =
              newInternalRow(UTF8String.fromString(from), snapshotBefore, snapshotAfter);
          return asScanIterator(OUTPUT_TYPE, outputRow);
        });
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String description() {
    return "FastForwardBranchProcedure";
  }
}
