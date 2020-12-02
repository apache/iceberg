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

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.actions.SnapshotAction;
import org.apache.iceberg.actions.Spark3SnapshotAction;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.Spark3Util.CatalogAndIdentifier;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.runtime.BoxedUnit;

class SnapshotProcedure extends BaseProcedure {
  private static final DataType MAP = DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType);
  private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[]{
      ProcedureParameter.required("snapshot_source", DataTypes.StringType),
      ProcedureParameter.required("table", DataTypes.StringType),
      ProcedureParameter.optional("table_location", DataTypes.StringType),
      ProcedureParameter.optional("table_options", MAP)
  };

  private static final StructType OUTPUT_TYPE = new StructType(new StructField[]{
      new StructField("num_datafiles_included", DataTypes.LongType, false, Metadata.empty())
  });

  private SnapshotProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  public static SparkProcedures.ProcedureBuilder builder() {
    return new BaseProcedure.Builder<SnapshotProcedure>() {
      @Override
      protected SnapshotProcedure doBuild() {
        return new SnapshotProcedure(tableCatalog());
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
    String dest = args.getString(1);

    String snapshotLocation = args.isNullAt(2) ? null : args.getString(2);

    Map<String, String> options = new HashMap<>();
    if (!args.isNullAt(3)) {
      args.getMap(3).foreach(DataTypes.StringType, DataTypes.StringType,
          (k, v) -> {
            options.put(k.toString(), v.toString());
            return BoxedUnit.UNIT;
          });
    }

    CatalogAndIdentifier sourceIdent = toCatalogAndIdentifer(source, PARAMETERS[0].name(), tableCatalog());
    CatalogAndIdentifier destIdent = toCatalogAndIdentifer(dest, PARAMETERS[1].name(), tableCatalog());

    Preconditions.checkArgument(sourceIdent != destIdent || sourceIdent.catalog() != destIdent.catalog(),
        "Cannot create a snapshot with the same name as the source of the snapshot.");
    SnapshotAction action =  new Spark3SnapshotAction(spark(), sourceIdent.catalog(), sourceIdent.identifier(),
        destIdent.catalog(), destIdent.identifier());

    long numFiles;
    if (snapshotLocation != null) {
      numFiles = action.withLocation(snapshotLocation).withProperties(options).execute();
    } else {
      numFiles = action.withProperties(options).execute();
    }

    return new InternalRow[] {newInternalRow(numFiles)};
  }

  @Override
  public String description() {
    return "Creates an Iceberg table from a Spark Table. The Created table will be isolated from the original table" +
        "and can be modified without modifying the original table.";
  }

}
