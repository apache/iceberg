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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.ExpressionInfo
import org.apache.spark.sql.types.UDTRegistration
import org.locationtech.jts.geom.Geometry
import scala.reflect.ClassTag

object TestingGeospatialLibraryInitializer {
  def initialize(spark: SparkSession): Unit = {
    UDTRegistration.register(classOf[Geometry].getName, classOf[GeometryUDT].getName)
    registerFunction(spark, args => ST_Intersects(args.head, args.last))
    registerFunction(spark, args => ST_Covers(args.head, args.last))
    registerFunction(spark, ST_PolygonFromEnvelope)
    registerFunction(spark, ST_Buffer)
  }

  private def registerFunction[C <: Expression : ClassTag](spark: SparkSession,
                                                  constructor: Seq[Expression] => C): Unit = {
    val classTag = implicitly[ClassTag[C]]
    val functionName = classTag.runtimeClass.getSimpleName
    val functionIdentifier = FunctionIdentifier(functionName)
    val expressionInfo = new ExpressionInfo(classTag.runtimeClass.getCanonicalName,
      functionIdentifier.database.orNull, functionName)
    spark.sessionState.functionRegistry.registerFunction(
      functionIdentifier, expressionInfo, constructor)
  }
}
