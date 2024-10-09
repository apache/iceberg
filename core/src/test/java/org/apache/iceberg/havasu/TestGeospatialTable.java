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
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTableTestBase;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.havasu.GeometryEncoding;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestGeospatialTable extends HadoopTableTestBase {

  public static Stream<Arguments> parameters() {
    return Stream.of(
        Arguments.of("ewkb"), Arguments.of("wkb"), Arguments.of("wkt"), Arguments.of("geojson"));
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void testCreateGeospatialTable(String encodingName) throws IOException {
    GeometryEncoding encoding = GeometryEncoding.fromName(encodingName);
    Schema schema =
        new Schema(
            required(3, "id", Types.IntegerType.get(), "unique ID"),
            required(4, "data", Types.StringType.get()),
            required(5, "geom", Types.GeometryType.get(encoding), "geospatial column"));

    TableIdentifier identifier = TableIdentifier.of("a", "geos_t1");
    try (HadoopCatalog catalog = hadoopCatalog()) {
      catalog.createTable(identifier, schema);
      Table table = catalog.loadTable(identifier);
      Types.NestedField geomField = table.schema().findField("geom");
      Assertions.assertEquals(geomField.type().typeId(), Type.TypeID.GEOMETRY);
      Assertions.assertEquals(((Types.GeometryType) geomField.type()).encoding(), encoding);
      Assertions.assertTrue(catalog.dropTable(identifier));
    }
  }

  @Test
  public void testPromoteToGeometryFields() throws IOException {
    TableIdentifier identifier = TableIdentifier.of("a", "geos_t2");
    Schema schema =
        new Schema(
            required(3, "id", Types.IntegerType.get()),
            required(4, "geom_0", Types.StringType.get()),
            required(5, "geom_1", Types.StringType.get()),
            required(6, "geom_2", Types.BinaryType.get()),
            required(7, "geom_3", Types.BinaryType.get()));
    try (HadoopCatalog catalog = hadoopCatalog()) {
      catalog.createTable(identifier, schema);
      Table table = catalog.loadTable(identifier);
      table
          .updateSchema()
          .updateColumn("geom_0", Types.GeometryType.get("wkt"))
          .updateColumn("geom_1", Types.GeometryType.get("geojson"))
          .updateColumn("geom_2", Types.GeometryType.get("wkb"))
          .updateColumn("geom_3", Types.GeometryType.get("ewkb"))
          .commit();
      Schema newSchema = table.schema();
      Assertions.assertEquals(Type.TypeID.GEOMETRY, newSchema.findType("geom_0").typeId());
      Assertions.assertEquals("wkt", getEncodingName(newSchema, "geom_0"));
      Assertions.assertEquals(Type.TypeID.GEOMETRY, newSchema.findType("geom_1").typeId());
      Assertions.assertEquals("geojson", getEncodingName(newSchema, "geom_1"));
      Assertions.assertEquals(Type.TypeID.GEOMETRY, newSchema.findType("geom_2").typeId());
      Assertions.assertEquals("wkb", getEncodingName(newSchema, "geom_2"));
      Assertions.assertEquals(Type.TypeID.GEOMETRY, newSchema.findType("geom_3").typeId());
      Assertions.assertEquals("ewkb", getEncodingName(newSchema, "geom_3"));

      table
          .updateSchema()
          .updateColumn("geom_0", Types.StringType.get())
          .updateColumn("geom_1", Types.StringType.get())
          .updateColumn("geom_2", Types.BinaryType.get())
          .updateColumn("geom_3", Types.BinaryType.get())
          .commit();
      newSchema = table.schema();
      Assertions.assertEquals(Type.TypeID.STRING, newSchema.findType("geom_0").typeId());
      Assertions.assertEquals(Type.TypeID.STRING, newSchema.findType("geom_1").typeId());
      Assertions.assertEquals(Type.TypeID.BINARY, newSchema.findType("geom_2").typeId());
      Assertions.assertEquals(Type.TypeID.BINARY, newSchema.findType("geom_3").typeId());
    }
  }

  private String getEncodingName(Schema schema, String fieldName) {
    Types.GeometryType geomType = (Types.GeometryType) schema.findType(fieldName);
    return geomType.encoding().encoding();
  }
}
