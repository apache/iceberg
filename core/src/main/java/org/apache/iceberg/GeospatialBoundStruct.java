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
package org.apache.iceberg;

import java.util.List;
import org.apache.iceberg.avro.SupportsIndexProjection;
import org.apache.iceberg.types.Types;

class GeospatialBoundStruct extends SupportsIndexProjection {
  @SuppressWarnings("MemberName")
  private Double x = null;

  @SuppressWarnings("MemberName")
  private Double y = null;

  @SuppressWarnings("MemberName")
  private Double z = null;

  @SuppressWarnings("MemberName")
  private Double m = null;

  GeospatialBoundStruct(Types.StructType projection) {
    super(coordinatePositions(projection));
  }

  private static int[] coordinatePositions(Types.StructType projection) {
    List<Types.NestedField> fields = projection.fields();
    int[] positions = new int[fields.size()];
    for (int i = 0; i < positions.length; i += 1) {
      positions[i] =
          switch (fields.get(i).name()) {
            case StatsUtil.GEO_BOUND_X_NAME -> 0;
            case StatsUtil.GEO_BOUND_Y_NAME -> 1;
            case StatsUtil.GEO_BOUND_Z_NAME -> 2;
            case StatsUtil.GEO_BOUND_M_NAME -> 3;
            default ->
                throw new IllegalArgumentException("Unknown geo bound field: " + fields.get(i));
          };
    }

    return positions;
  }

  private GeospatialBoundStruct(GeospatialBoundStruct toCopy) {
    super(toCopy);
    this.x = toCopy.x;
    this.y = toCopy.y;
    this.z = toCopy.z;
    this.m = toCopy.m;
  }

  @SuppressWarnings("MethodName")
  double x() {
    return x;
  }

  @SuppressWarnings("MethodName")
  double y() {
    return y;
  }

  @SuppressWarnings("MethodName")
  double z() {
    return z != null ? z : Double.NaN;
  }

  @SuppressWarnings("MethodName")
  double m() {
    return m != null ? m : Double.NaN;
  }

  GeospatialBoundStruct copy() {
    return new GeospatialBoundStruct(this);
  }

  @Override
  protected <T> T internalGet(int pos, Class<T> javaClass) {
    return javaClass.cast(getByPos(pos));
  }

  private Double getByPos(int pos) {
    return switch (pos) {
      case 0 -> x;
      case 1 -> y;
      case 2 -> z;
      case 3 -> m;
      default -> throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    };
  }

  @Override
  protected <T> void internalSet(int pos, T value) {
    switch (pos) {
      case 0 -> this.x = (Double) value;
      case 1 -> this.y = (Double) value;
      case 2 -> this.z = (Double) value;
      case 3 -> this.m = (Double) value;
      default -> throw new UnsupportedOperationException("Unknown field ordinal: " + pos);
    }
  }
}
