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
package org.apache.iceberg.parquet;

import org.apache.iceberg.Geography;
import org.apache.iceberg.util.GeometryUtil;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBWriter;

public class ParquetGeographyValueWriters {
  private ParquetGeographyValueWriters() {}

  public static ParquetValueWriters.PrimitiveWriter<Geography> buildWriter(ColumnDescriptor desc) {
    return new GeographyWriter(desc);
  }

  private static class GeographyWriter extends ParquetValueWriters.PrimitiveWriter<Geography> {

    private final WKBWriter[] wkbWriters = {
      new WKBWriter(2, false), new WKBWriter(3, false), new WKBWriter(4, false)
    };

    GeographyWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int rl, Geography geog) {
      Geometry geom = geog.geometry();
      int numDimensions = GeometryUtil.getOutputDimension(geom);
      byte[] wkb = wkbWriters[numDimensions - 2].write(geom);
      column.writeBinary(rl, Binary.fromReusedByteArray(wkb));
    }
  }
}
