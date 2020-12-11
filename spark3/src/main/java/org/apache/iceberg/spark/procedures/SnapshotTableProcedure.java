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

import java.util.Map;
import org.apache.iceberg.actions.SnapshotAction;
import org.apache.iceberg.actions.Spark3SnapshotAction;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.Spark3Util.CatalogAndIdentifier;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.runtime.BoxedUnit;

class SnapshotTableProcedure extends BaseProcedure {
  private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[]{
      ProcedureParameter.required("source_table", DataTypes.StringType),
      ProcedureParameter.required("table", DataTypes.StringType),
      ProcedureParameter.optional("location", DataTypes.StringType),
      ProcedureParameter.optional("properties", STRING_MAP)
  };

  private static final StructType OUTPUT_TYPE = new StructType(new StructField[]{
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
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public StructType outputType() {
    return OUTPUT_TYPE;
  }

  @Override
  public InternalRow[] call(InternalRow args) {
    String source = args.getString(0);
    CatalogAndIdentifier sourceIdent = toCatalogAndIdentifier(source, PARAMETERS[0].name(), tableCatalog());

    String dest = args.getString(1);
    CatalogAndIdentifier destIdent = toCatalogAndIdentifier(dest, PARAMETERS[1].name(), tableCatalog());

    String snapshotLocation = args.isNullAt(2) ? null : args.getString(2);

    Map<String, String> properties = Maps.newHashMap();
    if (!args.isNullAt(3)) {
      args.getMap(3).foreach(DataTypes.StringType, DataTypes.StringType,
          (k, v) -> {
            properties.put(k.toString(), v.toString());
            return BoxedUnit.UNIT;
          });
    }

    Preconditions.checkArgument(sourceIdent != destIdent || sourceIdent.catalog() != destIdent.catalog(),
        "Cannot create a snapshot with the same name as the source of the snapshot.");
    SnapshotAction action = new Spark3SnapshotAction(spark(), sourceIdent.catalog(), sourceIdent.identifier(),
        destIdent.catalog(), destIdent.identifier());

    if (snapshotLocation != null) {
      action.withLocation(snapshotLocation);
    }

    long importedDataFiles = action.withProperties(properties).execute();
    return new InternalRow[] {newInternalRow(importedDataFiles)};
  }

  @Override
  public String description() {
    return "SnapshotTableProcedure";
  }

}
