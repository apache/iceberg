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
package org.apache.spark.sql.catalyst.optimizer

import java.lang.{Double => JDouble, Float => JFloat, Integer => JInteger, Long => JLong}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.execution.RecordBinaryComparator
import org.apache.spark.sql.types._

private[sql] object ChangelogValueUtil {
  def containsFloatingPoint(dataType: DataType): Boolean = dataType match {
    case FloatType | DoubleType => true
    case ArrayType(elementType, _) => containsFloatingPoint(elementType)
    case MapType(keyType, valueType, _) =>
      containsFloatingPoint(keyType) || containsFloatingPoint(valueType)
    case struct: StructType => struct.fields.exists(field => containsFloatingPoint(field.dataType))
    case _ => false
  }

  /**
   * Identifies changelog values with distinct signed zeros, equal NaNs regardless of payload, and
   * maps independent of entry order. Unlike signed zeros, NaNs also compare equal in Spark.
   */
  def canonicalize(expression: Expression): Expression = {
    if (!containsFloatingPoint(expression.dataType)) {
      return InsertMapSortExpression.insertMapSortRecursively(expression)
    }

    expression.dataType match {
      case FloatType =>
        StaticInvoke(
          classOf[JFloat],
          IntegerType,
          "floatToIntBits",
          Seq(expression),
          returnNullable = false)
      case DoubleType =>
        StaticInvoke(
          classOf[JDouble],
          LongType,
          "doubleToLongBits",
          Seq(expression),
          returnNullable = false)
      case _: ArrayType => ChangelogArrayKey(expression)
      case _: MapType =>
        MapSort(
          MapFromArrays(canonicalize(MapKeys(expression)), canonicalize(MapValues(expression))))
      case struct: StructType =>
        val fields = struct.fields.zipWithIndex.flatMap { case (field, index) =>
          Seq(Literal(field.name), canonicalize(GetStructField(expression, index)))
        }
        val result = CreateNamedStruct(fields.toSeq)
        If(IsNull(expression), Literal.create(null, result.dataType), result)
      case _ => expression
    }
  }

  /**
   * Compares records with the same equality semantics as [[canonicalize]].
   */
  def createOrdering(schema: StructType, indices: Array[Int]): BaseOrdering = {
    val keys = indices.map { index =>
      val reference = BoundReference(index, schema(index).dataType, nullable = true)
      canonicalize(reference)
    }.toSeq
    // Variants support binary equality but not Spark ordering.
    if (keys.exists(_.dataType.existsRecursively(_ == VariantType))) {
      new BaseOrdering {
        private val leftProjection = UnsafeProjection.create(keys)
        private val rightProjection = UnsafeProjection.create(keys)
        private val comparator = new RecordBinaryComparator

        override def compare(left: InternalRow, right: InternalRow): Int = {
          val leftKey = leftProjection(left)
          val rightKey = rightProjection(right)
          comparator.compare(
            leftKey.getBaseObject,
            leftKey.getBaseOffset,
            leftKey.getSizeInBytes,
            rightKey.getBaseObject,
            rightKey.getBaseOffset,
            rightKey.getSizeInBytes)
        }
      }
    } else {
      RowOrdering.create(keys.map(SortOrder(_, Ascending)), Nil)
    }
  }
}

// ArrayTransform's generic getters can rebuild SQLConf for every element in an RDD task.
private[sql] case class ChangelogArrayKey(child: Expression)
    extends UnaryExpression
    with CodegenFallback {
  private lazy val arrayType = child.dataType.asInstanceOf[ArrayType]
  private lazy val elementKey = ChangelogValueUtil.canonicalize(
    BoundReference(0, arrayType.elementType, arrayType.containsNull))

  @transient private lazy val readKey: (ArrayData, Int) => Any = arrayType.elementType match {
    case FloatType =>
      (array, index) => JInteger.valueOf(JFloat.floatToIntBits(array.getFloat(index)))
    case DoubleType =>
      (array, index) => JLong.valueOf(JDouble.doubleToLongBits(array.getDouble(index)))
    case elementType =>
      val inputRow = new GenericInternalRow(1)
      val readElement = InternalRow.getAccessor(elementType, arrayType.containsNull)
      val projection = MutableProjection.create(Seq(elementKey))
      val readProjected = InternalRow.getAccessor(elementKey.dataType, arrayType.containsNull)
      (array, index) => {
        inputRow.update(0, readElement(array, index))
        InternalRow.copyValue(readProjected(projection(inputRow), 0))
      }
  }

  override lazy val dataType: DataType = ArrayType(elementKey.dataType, arrayType.containsNull)

  override def nullIntolerant: Boolean = true

  override protected def nullSafeEval(value: Any): Any = {
    val array = value.asInstanceOf[ArrayData]
    val numElements = array.numElements()
    val keys = new Array[Any](numElements)
    var index = 0
    while (index < numElements) {
      if (!array.isNullAt(index)) {
        keys(index) = readKey(array, index)
      }
      index += 1
    }
    new GenericArrayData(keys)
  }

  override protected def withNewChildInternal(newChild: Expression): ChangelogArrayKey = {
    copy(child = newChild)
  }
}
