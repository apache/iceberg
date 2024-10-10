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
package org.apache.iceberg.util;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

public class GeometryUtil {

  private GeometryUtil() {}

  public static byte[] toWKB(Geometry geom) {
    WKBWriter wkbWriter = new WKBWriter(getDimension(geom), false);
    return wkbWriter.write(geom);
  }

  public static String toWKT(Geometry geom) {
    WKTWriter wktWriter = new WKTWriter(getDimension(geom));
    return wktWriter.write(geom);
  }

  public static Geometry fromWKB(byte[] wkb) {
    WKBReader reader = new WKBReader();
    try {
      return reader.read(wkb);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse WKB", e);
    }
  }

  public static Geometry fromWKT(String wkt) {
    WKTReader reader = new WKTReader();
    try {
      return reader.read(wkt);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse WKT", e);
    }
  }

  public static int getDimension(Geometry geom) {
    return geom.getCoordinate() != null && !java.lang.Double.isNaN(geom.getCoordinate().getZ())
        ? 3
        : 2;
  }
}
