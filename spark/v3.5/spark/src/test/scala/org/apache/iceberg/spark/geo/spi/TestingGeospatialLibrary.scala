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
package org.apache.iceberg.spark.geo.spi

import org.apache.iceberg.expressions.{Expressions => IcebergExpressions}
import org.apache.iceberg.spark.geo.testing.GeometryUDT
import org.apache.iceberg.spark.geo.testing.ST_Covers
import org.apache.iceberg.spark.geo.testing.ST_Intersects
import org.apache.iceberg.spark.geo.testing.ST_Predicate
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.execution.datasources.PushableColumn
import org.apache.spark.sql.execution.datasources.PushableColumnBase
import org.apache.spark.sql.types.DataType
import org.locationtech.jts.geom.Geometry

class TestingGeospatialLibrary extends GeospatialLibrary {
  def getGeometryType(): DataType = {
    GeometryUDT
  }

  def fromJTS(geometry: Geometry): AnyRef = {
    GeometryUDT.serialize(geometry).asInstanceOf[AnyRef]
  }

  def toJTS(geometry: Any): Geometry = {
    GeometryUDT.deserialize(geometry)
  }

  def isSpatialFilter(sparkExpression: Expression): Boolean = {
    sparkExpression.isInstanceOf[ST_Predicate]
  }

  def translateToIceberg(sparkExpression: Expression): org.apache.iceberg.expressions.Expression = {
    val pushableColumn = PushableColumn(nestedPredicatePushdownEnabled = true)
    sparkExpression match {
      case _: ST_Predicate =>
        val icebergExpr = {
          for ((name, value, literalOnRight) <- resolveNameAndLiteral(sparkExpression.children, pushableColumn))
            yield sparkExpression match {
              case _: ST_Intersects => IcebergExpressions.stIntersects(name, GeometryUDT.deserialize(value))
              case _: ST_Covers =>
                if (literalOnRight) {
                  IcebergExpressions.stCovers(name, GeometryUDT.deserialize(value))
                } else {
                  IcebergExpressions.stIntersects(name, GeometryUDT.deserialize(value))
                }
            }
        }
        icebergExpr.orNull
      case _ => null
    }
  }

  private def resolveNameAndLiteral(expressions: Seq[Expression],
                                    pushableColumn: PushableColumnBase): Option[(String, Any, Boolean)] = {
    expressions match {
      case Seq(pushableColumn(name), Literal(v, _)) => Some(name, v, true)
      case Seq(Literal(v, _), pushableColumn(name)) => Some(name, v, false)
      case _ => None
    }
  }
}
