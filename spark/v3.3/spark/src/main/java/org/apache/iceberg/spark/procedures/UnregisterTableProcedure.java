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

import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.iceberg.spark.source.HasIcebergCatalog;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

class UnregisterTableProcedure extends BaseProcedure {
  private static final ProcedureParameter[] PARAMETERS =
      new ProcedureParameter[] {ProcedureParameter.required("table", DataTypes.StringType)};

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("isUnregistered", DataTypes.BooleanType, false, Metadata.empty())
          });

  private UnregisterTableProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  public static ProcedureBuilder builder() {
    return new Builder<UnregisterTableProcedure>() {
      @Override
      protected UnregisterTableProcedure doBuild() {
        return new UnregisterTableProcedure(tableCatalog());
      }
    };
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
    TableIdentifier identifier =
        Spark3Util.identifierToTableIdentifier(toIdentifier(args.getString(0), "table"));
    Preconditions.checkArgument(
        tableCatalog() instanceof HasIcebergCatalog,
        "Cannot use Unregister Table in a non-Iceberg catalog");

    Catalog icebergCatalog = ((HasIcebergCatalog) tableCatalog()).icebergCatalog();
    if (isHadoopCatalog(icebergCatalog)) {
      // even with purge as false, dropTable() will clean the files for hadoop catalog.
      throw new UnsupportedOperationException(
          "Unregister table is not supported for Hadoop catalog.");
    }

    // with purge as false, dropTable() will not load the table metadata.
    // It just drops the table entry from catalog.
    boolean isUnregistered = icebergCatalog.dropTable(identifier, false);

    return new InternalRow[] {newInternalRow(isUnregistered)};
  }

  private boolean isHadoopCatalog(Catalog icebergCatalog) {
    return icebergCatalog instanceof HadoopCatalog
        || (icebergCatalog instanceof CachingCatalog
            && ((CachingCatalog) icebergCatalog).catalog() instanceof HadoopCatalog);
  }

  @Override
  public String description() {
    return "UnregisterTableProcedure";
  }
}
