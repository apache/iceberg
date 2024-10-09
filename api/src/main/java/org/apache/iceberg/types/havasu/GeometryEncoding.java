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
package org.apache.iceberg.types.havasu;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;

/** Encoding of geometry column */
public enum GeometryEncoding {
  EWKB("ewkb", TypeID.BINARY),
  WKB("wkb", TypeID.BINARY),
  WKT("wkt", TypeID.STRING),
  GEOJSON("geojson", TypeID.STRING);

  public static final GeometryEncoding DEFAULT_ENCODING = EWKB;

  private final String encoding;
  private final TypeID physicalTypeId;

  GeometryEncoding(String encoding, TypeID physicalType) {
    this.encoding = encoding;
    this.physicalTypeId = physicalType;
  }

  public String encoding() {
    return encoding;
  }

  public TypeID physicalTypeId() {
    return physicalTypeId;
  }

  public Type physicalType() {
    switch (physicalTypeId) {
      case BINARY:
        return Types.BinaryType.get();
      case STRING:
        return Types.StringType.get();
      default:
        throw new IllegalArgumentException(
            "Unknown physical type for geometry encoding: " + encoding);
    }
  }

  public static GeometryEncoding fromName(String encoding) {
    for (GeometryEncoding ge : GeometryEncoding.values()) {
      if (ge.encoding.equals(encoding)) {
        return ge;
      }
    }
    throw new IllegalArgumentException("Unknown geometry encoding: " + encoding);
  }
}
