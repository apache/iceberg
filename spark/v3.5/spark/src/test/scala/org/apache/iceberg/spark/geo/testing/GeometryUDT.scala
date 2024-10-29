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
package org.apache.iceberg.spark.geo.testing

import org.apache.iceberg.util.GeometryUtil
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.UDTRegistration
import org.apache.spark.sql.types.UserDefinedType
import org.locationtech.jts.geom.Geometry

/** A Geometry UDT for testing the geometry library integration support in Iceberg Spark. */
class GeometryUDT extends UserDefinedType[Geometry] {

  def sqlType(): DataType = DataTypes.BinaryType

  def serialize(obj: Geometry): Any = GeometryUtil.toWKB(obj)

  def deserialize(datum: Any): Geometry = {
    datum match {
      case bytes: Array[Byte] => GeometryUtil.fromWKB(bytes)
      case _ => throw new IllegalArgumentException(
        s"Expected a Geometry object but got ${datum.getClass.getName}")
    }
  }

  def userClass(): Class[Geometry] = {
    classOf[Geometry]
  }
}

object GeometryUDT extends GeometryUDT
