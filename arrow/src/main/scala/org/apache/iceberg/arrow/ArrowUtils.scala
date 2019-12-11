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

package org.apache.iceberg.arrow

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.{DateUnit, FloatingPointPrecision, TimeUnit}
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType,
  DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType,
  ShortType, StringType, StructField, StructType, TimestampType}
import scala.collection.JavaConverters._

/**
 * This code is copied from Apache Spark Project's SQL ArrowUtils to be able
  * to shade the Arrow dependencies and keep Arrow code independent from Spark
 */
object ArrowUtils {

  val rootAllocator = new RootAllocator(Long.MaxValue)

  // todo: support more types.
  // scalastyle:off cyclomatic.complexity
  def fromArrowType(dt: ArrowType): DataType = dt match {
    case ArrowType.Bool.INSTANCE => BooleanType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 => ByteType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 2 => ShortType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 4 => IntegerType
    case int: ArrowType.Int if int.getIsSigned && int.getBitWidth == 8 * 8 => LongType
    case float: ArrowType.FloatingPoint if float.getPrecision() == FloatingPointPrecision.SINGLE => FloatType
    case float: ArrowType.FloatingPoint if float.getPrecision() == FloatingPointPrecision.DOUBLE => DoubleType
    case ArrowType.Utf8.INSTANCE => StringType
    case ArrowType.Binary.INSTANCE => BinaryType
    case d: ArrowType.Decimal => DecimalType(d.getPrecision, d.getScale)
    case date: ArrowType.Date if date.getUnit == DateUnit.DAY => DateType
    case ts: ArrowType.Timestamp if ts.getUnit == TimeUnit.MICROSECOND => TimestampType
    case _ => throw new UnsupportedOperationException(s"Unsupported data type: $dt")
  }
  // scalastyle:on cyclomatic.complexity

  def fromArrowField(field: Field): DataType = {
    field.getType match {
      case ArrowType.List.INSTANCE =>
        val elementField = field.getChildren().get(0)
        val elementType = fromArrowField(elementField)
        ArrayType(elementType, containsNull = elementField.isNullable)
      case ArrowType.Struct.INSTANCE =>
        val fields = field.getChildren().asScala.map { child =>
          val dt = fromArrowField(child)
          StructField(child.getName, dt, child.isNullable)
        }
        StructType(fields)
      case arrowType: ArrowType => fromArrowType(arrowType)
    }
  }

  /** Maps schema from Spark to Arrow. NOTE: timeZoneId required for TimestampType in StructType */
  def fromArrowSchema(schema: Schema): StructType = {
    StructType(schema.getFields.asScala.map { field =>
      val dt = fromArrowField(field)
      StructField(field.getName, dt, field.isNullable)
    })
  }

  /** Return Map with conf settings to be used in ArrowPythonRunner */
  def getPythonRunnerConfMap(conf: SQLConf): Map[String, String] = {
    val timeZoneConf = if (conf.pandasRespectSessionTimeZone) {
      Seq(SQLConf.SESSION_LOCAL_TIMEZONE.key -> conf.sessionLocalTimeZone)
    } else {
      Nil
    }
    val pandasColsByName = Seq(SQLConf.PANDAS_GROUPED_MAP_ASSIGN_COLUMNS_BY_NAME.key ->
      conf.pandasGroupedMapAssignColumnsByName.toString)
    Map(timeZoneConf ++ pandasColsByName: _*)
  }
}
