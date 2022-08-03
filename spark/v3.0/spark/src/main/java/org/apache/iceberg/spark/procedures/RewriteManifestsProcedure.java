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
import org.apache.iceberg.actions.RewriteManifests;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * A procedure that rewrites manifests in a table.
 *
 * <p><em>Note:</em> this procedure invalidates all cached Spark plans that reference the affected
 * table.
 *
 * @see SparkActions#rewriteManifests(Table) ()
 */
class RewriteManifestsProcedure extends BaseProcedure {

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("table", DataTypes.StringType),
        ProcedureParameter.optional("use_caching", DataTypes.BooleanType)
      };

  // counts are not nullable since the action result is never null
  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField(
                "rewritten_manifests_count", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("added_manifests_count", DataTypes.IntegerType, false, Metadata.empty())
          });

  public static ProcedureBuilder builder() {
    return new BaseProcedure.Builder<RewriteManifestsProcedure>() {
      @Override
      protected RewriteManifestsProcedure doBuild() {
        return new RewriteManifestsProcedure(tableCatalog());
      }
    };
  }

  private RewriteManifestsProcedure(TableCatalog tableCatalog) {
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

    return modifyIcebergTable(
        tableIdent,
        table -> {
          RewriteManifests action = actions().rewriteManifests(table);

          if (useCaching != null) {
            action.option("use-caching", useCaching.toString());
          }

          RewriteManifests.Result result = action.execute();

          return toOutputRows(result);
        });
  }

  private InternalRow[] toOutputRows(RewriteManifests.Result result) {
    int rewrittenManifestsCount = Iterables.size(result.rewrittenManifests());
    int addedManifestsCount = Iterables.size(result.addedManifests());
    InternalRow row = newInternalRow(rewrittenManifestsCount, addedManifestsCount);
    return new InternalRow[] {row};
  }

  @Override
  public String description() {
    return "RewriteManifestsProcedure";
  }
}
