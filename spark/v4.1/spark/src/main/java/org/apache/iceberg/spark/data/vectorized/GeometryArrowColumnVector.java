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
package org.apache.iceberg.spark.data.vectorized;

import org.apache.iceberg.arrow.vectorized.VectorHolder;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.util.STUtils;
import org.apache.spark.sql.types.GeometryType$;
import org.apache.spark.unsafe.types.GeometryVal;

class GeometryArrowColumnVector extends IcebergArrowColumnVector {
  private final int srid;

  GeometryArrowColumnVector(VectorHolder holder) {
    super(holder);
    Types.GeometryType geometryType = (Types.GeometryType) holder.icebergType();
    this.srid = GeometryType$.MODULE$.apply(geometryType.crs()).srid();
  }

  @Override
  public GeometryVal getGeometry(int rowId) {
    byte[] wkb = getBinary(rowId);
    return wkb == null ? null : STUtils.stGeomFromWKB(wkb, srid);
  }
}
