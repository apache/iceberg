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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.BinaryExpression
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.ImplicitCastInputTypes
import org.apache.spark.sql.catalyst.expressions.NullIntolerant
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DoubleType
import org.locationtech.jts.geom.Envelope
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory

abstract class ST_Predicate extends BinaryExpression with CodegenFallback {
  override def dataType: DataType = BooleanType

  protected def evalSpatialPredicate(geom1: Geometry, geom2: Geometry): Boolean

  override def nullSafeEval(input1: Any, input2: Any): Any = {
    val geom1 = GeometryUDT.deserialize(input1)
    val geom2 = GeometryUDT.deserialize(input2)
    evalSpatialPredicate(geom1, geom2)
  }
}

case class ST_Intersects(left: Expression, right: Expression) extends ST_Predicate {

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }

  override protected def evalSpatialPredicate(geom1: Geometry, geom2: Geometry): Boolean = {
    geom1.intersects(geom2)
  }
}

case class ST_Covers(left: Expression, right: Expression) extends ST_Predicate {

  override protected def withNewChildrenInternal(newLeft: Expression, newRight: Expression): Expression = {
    copy(left = newLeft, right = newRight)
  }

  override protected def evalSpatialPredicate(geom1: Geometry, geom2: Geometry): Boolean = {
    geom1.covers(geom2)
  }
}

case class ST_PolygonFromEnvelope(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes with CodegenFallback {
  override def foldable: Boolean = true

  override def nullable: Boolean = true

  override def dataType: DataType = GeometryUDT

  override def inputTypes: Seq[DataType] = Seq(DoubleType, DoubleType, DoubleType, DoubleType)

  override def eval(input: InternalRow): Any = {
    val minX = children(0).eval(input).asInstanceOf[Double]
    val minY = children(1).eval(input).asInstanceOf[Double]
    val maxX = children(2).eval(input).asInstanceOf[Double]
    val maxY = children(3).eval(input).asInstanceOf[Double]
    val factory = new GeometryFactory()
    val geom = factory.toGeometry(new Envelope(minX, maxX, minY, maxY))
    GeometryUDT.serialize(geom)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)
}

case class ST_Buffer(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes with CodegenFallback with NullIntolerant {
  override def foldable: Boolean = true

  override def nullable: Boolean = true

  override def dataType: DataType = GeometryUDT

  override def inputTypes: Seq[DataType] = Seq(GeometryUDT, DoubleType)

  override def eval(input: InternalRow): Any = {
    val geom = GeometryUDT.deserialize(children(0).eval(input))
    val distance = children(1).eval(input).asInstanceOf[Double]
    val buffer = geom.buffer(distance)
    GeometryUDT.serialize(buffer)
  }

  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(children = newChildren)
}
