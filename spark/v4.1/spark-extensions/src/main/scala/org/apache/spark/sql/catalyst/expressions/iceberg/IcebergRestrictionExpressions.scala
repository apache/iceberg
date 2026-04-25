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
package org.apache.spark.sql.catalyst.expressions.iceberg

import java.nio.ByteBuffer
import java.util.Locale
import org.apache.iceberg.util.SerializableFunction
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.UnaryExpression
import org.apache.spark.sql.catalyst.expressions.codegen.Block.BlockHelper
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.expressions.codegen.ExprCode
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

/**
 * Generic opaque masking wrapper. Applies a bound masking function from
 * [[org.apache.iceberg.rest.restrictions.Actions]] to the child expression's value.
 *
 * Type bridging between Spark internal representation and Iceberg Java types:
 *  - String: UTF8String <-> String
 *  - Binary: byte[] <-> ByteBuffer
 *  - Decimal: Spark Decimal <-> BigDecimal
 *  - Int/Long/Float/Double/Boolean/Date/Timestamp: pass-through (autobox in codegen)
 *
 * Participates in whole-stage codegen: the conversions above and the opaque
 * `mask.apply(...)` call are emitted inline, so the masked scan stays within the
 * generated loop and primitive unboxing happens directly.
 */
case class IcebergRestricted(child: Expression, mask: SerializableFunction[Object, Object])
    extends UnaryExpression {

  override def prettyName: String = "iceberg_restricted"
  override def dataType: DataType = child.dataType
  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val rawValue = child.eval(input)
    val icebergValue = if (rawValue == null) null else toIceberg(rawValue, child.dataType)
    val result = mask.apply(icebergValue.asInstanceOf[Object])
    if (result == null) null else fromIceberg(result, child.dataType)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childGen = child.genCode(ctx)
    val maskRef =
      ctx.addReferenceObj("mask", mask, classOf[SerializableFunction[_, _]].getName)
    val maskResult = ctx.freshName("maskResult")
    val javaType = CodeGenerator.javaType(dataType)
    val defaultValue = CodeGenerator.defaultValue(dataType)
    val toIcebergExpr = toIcebergCode(childGen.value.toString, child.dataType)
    val fromIcebergStmt =
      fromIcebergAssignment(ev.value.toString, maskResult, child.dataType)

    val genCode =
      code"""
        |${childGen.code}
        |Object $maskResult = ${childGen.isNull}
        |    ? $maskRef.apply(null)
        |    : $maskRef.apply($toIcebergExpr);
        |boolean ${ev.isNull} = $maskResult == null;
        |$javaType ${ev.value} = $defaultValue;
        |if (!${ev.isNull}) {
        |  $fromIcebergStmt
        |}
        |""".stripMargin
    ev.copy(code = genCode)
  }

  private def toIceberg(value: Any, dt: DataType): Any = dt match {
    case StringType => value.asInstanceOf[UTF8String].toString
    case BinaryType => ByteBuffer.wrap(value.asInstanceOf[Array[Byte]])
    case _: DecimalType => value.asInstanceOf[Decimal].toJavaBigDecimal
    case _ => value
  }

  private def fromIceberg(value: Any, dt: DataType): Any = dt match {
    case StringType => UTF8String.fromString(value.toString)
    case BinaryType =>
      val bb = value.asInstanceOf[ByteBuffer]
      val arr = new Array[Byte](bb.remaining())
      bb.duplicate().get(arr)
      arr
    case dt: DecimalType =>
      Decimal(value.asInstanceOf[java.math.BigDecimal], dt.precision, dt.scale)
    case _ => value
  }

  private def toIcebergCode(valueExpr: String, dt: DataType): String = dt match {
    case StringType => s"$valueExpr.toString()"
    case BinaryType => s"java.nio.ByteBuffer.wrap($valueExpr)"
    case _: DecimalType => s"$valueExpr.toJavaBigDecimal()"
    case _ => valueExpr
  }

  private def fromIcebergAssignment(outVar: String, inVar: String, dt: DataType): String = {
    val helper = IcebergRestrictedCodegenSupport.getClass.getName.stripSuffix("$")
    dt match {
      case StringType =>
        s"$outVar = org.apache.spark.unsafe.types.UTF8String.fromString((String) $inVar);"
      case BinaryType =>
        // Helper sidesteps Janino's limited covariant-return handling on ByteBuffer.duplicate().
        s"$outVar = $helper.byteBufferToBytes($inVar);"
      case dt: DecimalType =>
        s"$outVar = $helper.toSparkDecimal($inVar, ${dt.precision}, ${dt.scale});"
      case t if CodeGenerator.isPrimitiveType(t) =>
        val boxed = CodeGenerator.boxedType(t)
        val prim = CodeGenerator.primitiveTypeName(t).toLowerCase(Locale.ROOT)
        s"$outVar = (($boxed) $inVar).${prim}Value();"
      case other =>
        throw new IllegalStateException(
          s"iceberg_restricted codegen has no mapping for type: $other " +
            s"(Action#bind should have rejected this type before reaching codegen)")
    }
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}

/** Helpers invoked from code generated by [[IcebergRestricted.doGenCode]]. */
object IcebergRestrictedCodegenSupport {
  def byteBufferToBytes(value: AnyRef): Array[Byte] = {
    val bb = value.asInstanceOf[java.nio.ByteBuffer]
    val arr = new Array[Byte](bb.remaining())
    bb.duplicate().get(arr)
    arr
  }

  def toSparkDecimal(value: AnyRef, precision: Int, scale: Int): Decimal =
    Decimal(value.asInstanceOf[java.math.BigDecimal], precision, scale)
}

/**
 * Opaque wrapper for the server-provided row filter. Hides the actual predicate
 * from EXPLAIN output so access-control policy details are not leaked.
 *
 * Pure pass-through at runtime: the child (a converted Catalyst boolean expression) does all
 * the work. Codegen delegates directly to the child so the filter participates in whole-stage
 * codegen; only the display-side overrides (prettyName/sql/simpleString/toString) stay opaque.
 */
case class IcebergRowFilterExpr(child: Expression) extends UnaryExpression {
  override def dataType: DataType = BooleanType
  override def nullable: Boolean = child.nullable
  override def prettyName: String = "iceberg_row_filter"

  override def sql: String = "iceberg_row_filter()"
  override def simpleString(maxFields: Int): String = "iceberg_row_filter()"
  override def toString: String = "iceberg_row_filter()"

  override protected def nullSafeEval(input: Any): Any = input

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childGen = child.genCode(ctx)
    ev.copy(code = childGen.code, isNull = childGen.isNull, value = childGen.value)
  }

  override protected def withNewChildInternal(newChild: Expression): Expression =
    copy(child = newChild)
}
