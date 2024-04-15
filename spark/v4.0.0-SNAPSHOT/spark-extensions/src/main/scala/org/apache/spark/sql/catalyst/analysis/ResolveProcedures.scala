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

package org.apache.spark.sql.catalyst.analysis

import java.util.Locale
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.Call
import org.apache.spark.sql.catalyst.plans.logical.CallArgument
import org.apache.spark.sql.catalyst.plans.logical.CallStatement
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.NamedArgument
import org.apache.spark.sql.catalyst.plans.logical.PositionalArgument
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.connector.catalog.LookupCatalog
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureCatalog
import org.apache.spark.sql.connector.iceberg.catalog.ProcedureParameter
import scala.collection.Seq

case class ResolveProcedures(spark: SparkSession) extends Rule[LogicalPlan] with LookupCatalog {

  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case CallStatement(CatalogAndIdentifier(catalog, ident), args) =>
      val procedure = catalog.asProcedureCatalog.loadProcedure(ident)

      val params = procedure.parameters
      val normalizedParams = normalizeParams(params)
      validateParams(normalizedParams)

      val normalizedArgs = normalizeArgs(args)
      Call(procedure, args = buildArgExprs(normalizedParams, normalizedArgs).toSeq)
  }

  private def validateParams(params: Seq[ProcedureParameter]): Unit = {
    // should not be any duplicate param names
    val duplicateParamNames = params.groupBy(_.name).collect {
      case (name, matchingParams) if matchingParams.length > 1 => name
    }

    if (duplicateParamNames.nonEmpty) {
      throw new AnalysisException(s"Duplicate parameter names: ${duplicateParamNames.mkString("[", ",", "]")}",
        Map.empty, None)
    }

    // optional params should be at the end
    params.sliding(2).foreach {
      case Seq(previousParam, currentParam) if !previousParam.required && currentParam.required =>
        throw new AnalysisException(
          s"Optional parameters must be after required ones but $currentParam is after $previousParam",
          Map.empty, None)
      case _ =>
    }
  }

  private def buildArgExprs(
      params: Seq[ProcedureParameter],
      args: Seq[CallArgument]): Seq[Expression] = {

    // build a map of declared parameter names to their positions
    val nameToPositionMap = params.map(_.name).zipWithIndex.toMap

    // build a map of parameter names to args
    val nameToArgMap = buildNameToArgMap(params, args, nameToPositionMap)

    // verify all required parameters are provided
    val missingParamNames = params.filter(_.required).collect {
      case param if !nameToArgMap.contains(param.name) => param.name
    }

    if (missingParamNames.nonEmpty) {
      throw new AnalysisException(s"Missing required parameters: ${missingParamNames.mkString("[", ",", "]")}",
        Map.empty, None)
    }

    val argExprs = new Array[Expression](params.size)

    nameToArgMap.foreach { case (name, arg) =>
      val position = nameToPositionMap(name)
      argExprs(position) = arg.expr
    }

    // assign nulls to optional params that were not set
    params.foreach {
      case p if !p.required && !nameToArgMap.contains(p.name) =>
        val position = nameToPositionMap(p.name)
        argExprs(position) = Literal.create(null, p.dataType)
      case _ =>
    }

    argExprs
  }

  private def buildNameToArgMap(
      params: Seq[ProcedureParameter],
      args: Seq[CallArgument],
      nameToPositionMap: Map[String, Int]): Map[String, CallArgument] = {

    val containsNamedArg = args.exists(_.isInstanceOf[NamedArgument])
    val containsPositionalArg = args.exists(_.isInstanceOf[PositionalArgument])

    if (containsNamedArg && containsPositionalArg) {
      throw new AnalysisException("Named and positional arguments cannot be mixed",
        Map.empty, None)
    }

    if (containsNamedArg) {
      buildNameToArgMapUsingNames(args, nameToPositionMap)
    } else {
      buildNameToArgMapUsingPositions(args, params)
    }
  }

  private def buildNameToArgMapUsingNames(
      args: Seq[CallArgument],
      nameToPositionMap: Map[String, Int]): Map[String, CallArgument] = {

    val namedArgs = args.asInstanceOf[Seq[NamedArgument]]

    val validationErrors = namedArgs.groupBy(_.name).collect {
      case (name, matchingArgs) if matchingArgs.size > 1 => s"Duplicate procedure argument: $name"
      case (name, _) if !nameToPositionMap.contains(name) => s"Unknown argument: $name"
    }

    if (validationErrors.nonEmpty) {
      throw new AnalysisException(s"Could not build name to arg map: ${validationErrors.mkString(", ")}",
        Map.empty, None)
    }

    namedArgs.map(arg => arg.name -> arg).toMap
  }

  private def buildNameToArgMapUsingPositions(
      args: Seq[CallArgument],
      params: Seq[ProcedureParameter]): Map[String, CallArgument] = {

    if (args.size > params.size) {
      throw new AnalysisException("Too many arguments for procedure",
        Map.empty, None)
    }

    args.zipWithIndex.map { case (arg, position) =>
      val param = params(position)
      param.name -> arg
    }.toMap
  }

  private def normalizeParams(params: Seq[ProcedureParameter]): Seq[ProcedureParameter] = {
    params.map {
      case param if param.required =>
        val normalizedName = param.name.toLowerCase(Locale.ROOT)
        ProcedureParameter.required(normalizedName, param.dataType)
      case param =>
        val normalizedName = param.name.toLowerCase(Locale.ROOT)
        ProcedureParameter.optional(normalizedName, param.dataType)
    }
  }

  private def normalizeArgs(args: Seq[CallArgument]): Seq[CallArgument] = {
    args.map {
      case a @ NamedArgument(name, _) => a.copy(name = name.toLowerCase(Locale.ROOT))
      case other => other
    }
  }

  implicit class CatalogHelper(plugin: CatalogPlugin) {
    def asProcedureCatalog: ProcedureCatalog = plugin match {
      case procedureCatalog: ProcedureCatalog =>
        procedureCatalog
      case _ =>
        throw new AnalysisException(s"Cannot use catalog ${plugin.name}: not a ProcedureCatalog",
          Map.empty, None)
    }
  }
}
