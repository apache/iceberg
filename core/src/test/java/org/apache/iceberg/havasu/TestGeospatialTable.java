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
package org.apache.iceberg.havasu;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTableTestBase;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGeospatialTable extends HadoopTableTestBase {

  @Test
  public void testCreateGeospatialTable() throws IOException {
    Schema schema =
        new Schema(
            required(3, "id", Types.IntegerType.get(), "unique ID"),
            required(4, "data", Types.StringType.get()),
            required(
                5,
                "geom",
                Types.GeometryType.of("test_crs", Types.GeometryType.Edges.SPHERICAL),
                "geospatial column"));

    TableIdentifier identifier = TableIdentifier.of("a", "geos_t1");
    try (HadoopCatalog catalog = hadoopCatalog()) {
      catalog.createTable(identifier, schema);
      Table table = catalog.loadTable(identifier);
      Types.NestedField geomField = table.schema().findField("geom");
      Assertions.assertEquals(geomField.type().typeId(), Type.TypeID.GEOMETRY);
      Types.GeometryType geomType = (Types.GeometryType) geomField.type();
      Assertions.assertEquals("test_crs", geomType.crs());
      Assertions.assertEquals(Types.GeometryType.Edges.SPHERICAL, geomType.edges());
      Assertions.assertTrue(catalog.dropTable(identifier));
    }
  }
}
