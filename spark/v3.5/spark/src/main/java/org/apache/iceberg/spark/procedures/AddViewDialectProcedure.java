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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.BaseCatalog;
import org.apache.iceberg.view.ReplaceViewVersion;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class AddViewDialectProcedure extends BaseProcedure {

  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {
        ProcedureParameter.required("view", DataTypes.StringType),
        ProcedureParameter.required("dialect", DataTypes.StringType),
        ProcedureParameter.required("sql", DataTypes.StringType)
      };

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("updated_version_id", DataTypes.IntegerType, true, Metadata.empty()),
          });

  public static SparkProcedures.ProcedureBuilder builder() {
    return new Builder<AddViewDialectProcedure>() {
      @Override
      protected AddViewDialectProcedure doBuild() {
        return new AddViewDialectProcedure(catalog());
      }
    };
  }

  private AddViewDialectProcedure(BaseCatalog catalog) {
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
    Identifier viewIdentifier = toIdentifier(args.getString(0), PARAMETERS[0].name());
    String dialect = args.getString(1);
    String sql = args.getString(2);

    Preconditions.checkArgument(!dialect.isEmpty(), "dialect should not be empty");
    Preconditions.checkArgument(!sql.isEmpty(), "sql should not be empty");

    return withIcebergView(
        viewIdentifier,
        view -> {
          ReplaceViewVersion replaceViewVersion = view.replaceVersion();
          replaceViewVersion
              .withSchema(view.schema())
              .withDefaultNamespace(view.currentVersion().defaultNamespace());

          // retain old representations
          view.currentVersion()
              .representations()
              .forEach(
                  representation -> {
                    SQLViewRepresentation viewRepresentation =
                        (SQLViewRepresentation) representation;
                    replaceViewVersion.withQuery(
                        viewRepresentation.dialect(), viewRepresentation.sql());
                  });

          // add new representation
          replaceViewVersion.withQuery(dialect, sql).commit();

          InternalRow outputRow = newInternalRow(view.currentVersion().versionId());
          return new InternalRow[] {outputRow};
        });
  }

  @Override
  public String description() {
    return "AddViewDialectProcedure";
  }
}
