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
import org.apache.spark.sql.catalyst.types.PhysicalDataType
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.util.GenericArrayData
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

  def canonicalize(expression: Expression): Expression = expression.dataType match {
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
    case _: ArrayType => transformArray(expression)
    case _: MapType =>
      MapSort(
        MapFromArrays(transformArray(MapKeys(expression)), transformArray(MapValues(expression))))
    case struct: StructType =>
      val fields = struct.fields.zipWithIndex.flatMap { case (field, index) =>
        Seq(Literal(field.name), canonicalize(GetStructField(expression, index)))
      }
      val result = CreateNamedStruct(fields.toSeq)
      If(IsNull(expression), Literal.create(null, result.dataType), result)
    case _ => expression
  }

  def createOrdering(schema: StructType, indices: Array[Int]): BaseOrdering = {
    val (floating, other) =
      indices.partition(index => containsFloatingPoint(schema(index).dataType))
    val orders = other.map { index =>
      SortOrder(
        InsertMapSortExpression.insertMapSortRecursively(
          BoundReference(index, schema(index).dataType, nullable = true)),
        Ascending)
    }
    val sparkOrdering = RowOrdering.create(orders.toSeq, Nil)
    if (floating.isEmpty) {
      sparkOrdering
    } else {
      val comparisons = floating.map(index => fieldOrdering(schema(index).dataType))
      new BaseOrdering {
        override def compare(left: InternalRow, right: InternalRow): Int = {
          val result = sparkOrdering.compare(left, right)
          if (result != 0) {
            return result
          }

          var index = 0
          while (index < floating.length) {
            val compared = comparisons(index).compare(left, floating(index), right, floating(index))
            if (compared != 0) {
              return compared
            }
            index += 1
          }
          0
        }
      }
    }
  }

  private trait FieldOrdering {
    def compare(
        left: SpecializedGetters,
        leftIndex: Int,
        right: SpecializedGetters,
        rightIndex: Int): Int
  }

  private def fieldOrdering(dataType: DataType): FieldOrdering = {
    val nonNull: FieldOrdering = dataType match {
      case FloatType =>
        (left, li, right, ri) => JFloat.compare(left.getFloat(li), right.getFloat(ri))
      case DoubleType =>
        (left, li, right, ri) => JDouble.compare(left.getDouble(li), right.getDouble(ri))
      case struct: StructType =>
        val fields = struct.fields.map(field => fieldOrdering(field.dataType))
        (left, li, right, ri) =>
          compareFields(
            left.getStruct(li, fields.length),
            right.getStruct(ri, fields.length),
            fields)
      case ArrayType(elementType, _) =>
        val element = fieldOrdering(elementType)
        (left, li, right, ri) => {
          val a = left.getArray(li)
          val b = right.getArray(ri)
          compareElements(a, b, a.numElements(), b.numElements(), element)
        }
      case mapType: MapType =>
        val input = BoundReference(0, mapType, nullable = true)
        val sorted = if (containsFloatingPoint(mapType.keyType)) {
          MapSort(MapFromArrays(transformArray(MapKeys(input)), MapValues(input)))
        } else {
          MapSort(input)
        }
        val key = fieldOrdering(sorted.dataType.asInstanceOf[MapType].keyType)
        val value = fieldOrdering(mapType.valueType)
        val inputRow = new GenericInternalRow(1)
        val sortMap = MutableProjection.create(Seq(sorted))
        (left, li, right, ri) => {
          inputRow.update(0, left.getMap(li))
          val a = sortMap(inputRow).getMap(0)
          inputRow.update(0, right.getMap(ri))
          val b = sortMap(inputRow).getMap(0)
          val keys =
            compareElements(a.keyArray(), b.keyArray(), a.numElements(), b.numElements(), key)
          if (keys != 0) {
            keys
          } else {
            compareElements(a.valueArray(), b.valueArray(), a.numElements(), b.numElements(), value)
          }
        }
      case _ =>
        val ordering = PhysicalDataType.ordering(dataType)
        val accessor = InternalRow.getAccessor(dataType, nullable = false)
        (left, li, right, ri) => ordering.compare(accessor(left, li), accessor(right, ri))
    }
    (left, li, right, ri) => {
      if (left.isNullAt(li)) {
        if (right.isNullAt(ri)) 0 else -1
      } else if (right.isNullAt(ri)) {
        1
      } else {
        nonNull.compare(left, li, right, ri)
      }
    }
  }

  private def compareFields(
      left: InternalRow,
      right: InternalRow,
      fields: Array[FieldOrdering]): Int = {
    var index = 0
    while (index < fields.length) {
      val result = fields(index).compare(left, index, right, index)
      if (result != 0) {
        return result
      }
      index += 1
    }
    0
  }

  private def compareElements(
      left: SpecializedGetters,
      right: SpecializedGetters,
      leftSize: Int,
      rightSize: Int,
      element: FieldOrdering): Int = {
    var index = 0
    while (index < math.min(leftSize, rightSize)) {
      val result = element.compare(left, index, right, index)
      if (result != 0) {
        return result
      }
      index += 1
    }
    JInteger.compare(leftSize, rightSize)
  }

  private def transformArray(expression: Expression): Expression = {
    val arrayType = expression.dataType.asInstanceOf[ArrayType]
    if (containsFloatingPoint(arrayType.elementType)) {
      ChangelogArrayKey(expression)
    } else {
      InsertMapSortExpression.insertMapSortRecursively(expression)
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

  @transient private lazy val inputRow = new GenericInternalRow(1)
  @transient private lazy val readElement =
    InternalRow.getAccessor(arrayType.elementType, arrayType.containsNull)
  @transient private lazy val projection = MutableProjection.create(Seq(elementKey))
  @transient private lazy val readKey =
    InternalRow.getAccessor(elementKey.dataType, arrayType.containsNull)

  override lazy val dataType: DataType = ArrayType(elementKey.dataType, arrayType.containsNull)

  override def nullIntolerant: Boolean = true

  override protected def nullSafeEval(value: Any): Any = {
    val array = value.asInstanceOf[ArrayData]
    val keys = new Array[Any](array.numElements())
    var index = 0
    while (index < array.numElements()) {
      if (!array.isNullAt(index)) {
        keys(index) = arrayType.elementType match {
          case FloatType => JInteger.valueOf(JFloat.floatToIntBits(array.getFloat(index)))
          case DoubleType => JLong.valueOf(JDouble.doubleToLongBits(array.getDouble(index)))
          case _ =>
            inputRow.update(0, readElement(array, index))
            InternalRow.copyValue(readKey(projection(inputRow), 0))
        }
      }
      index += 1
    }
    new GenericArrayData(keys)
  }

  override protected def withNewChildInternal(newChild: Expression): ChangelogArrayKey = {
    copy(child = newChild)
  }
}
