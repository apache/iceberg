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

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Call, CallArgument, CallStatement, LogicalPlan, NamedArgument, PositionalArgument}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogNotFoundException, CatalogPlugin, Identifier, Procedure, ProcedureCatalog, ProcedureParameter}
import scala.collection.Seq

object ResolveProcedures extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case CallStatement(nameParts, args) =>
      val (catalog, ident) = resolveCatalog(nameParts)

      val procedure = catalog.asProcedureCatalog.loadProcedure(ident)

      validateParams(procedure)
      validateMethodHandle(procedure)

      Call(procedure, argValues = buildArgValues(procedure, args))
  }

  private def validateParams(procedure: Procedure): Unit = {
    // should not be any duplicate param names
    val duplicateParamNames = procedure.parameters.groupBy(_.name).collect {
      case (name, matchingParams) if matchingParams.length > 1 => name
    }

    if (duplicateParamNames.nonEmpty) {
      throw new AnalysisException(s"Duplicate parameter names: ${duplicateParamNames.mkString("[", ",", "]")}")
    }

    // optional params should be at the end
    procedure.parameters.sliding(2).foreach {
      case Array(previousParam, currentParam) if previousParam.required && !currentParam.required =>
        throw new AnalysisException("Optional parameters must be after required ones")
      case _ =>
    }
  }

  private def validateMethodHandle(procedure: Procedure): Unit = {
    val params = procedure.parameters
    val outputType = procedure.outputType

    val methodHandle = procedure.methodHandle
    val methodType = methodHandle.`type`
    val methodReturnType = methodType.returnType

    // method cannot accept var ags
    if (methodHandle.isVarargsCollector) {
      throw new AnalysisException("Method must have fixed arity")
    }

    // verify the number of params in the procedure match the number of params in the method
    if (params.length != methodType.parameterCount) {
      throw new AnalysisException("Method parameter count must match the number of procedure parameters")
    }

    // the MethodHandle API does not allow us to check the generic type
    // so we only verify the return type is either void or iterable

    if (outputType.nonEmpty && methodReturnType != classOf[java.lang.Iterable[_]]) {
      throw new AnalysisException(
        s"Wrong method return type: $methodReturnType; the procedure defines $outputType " +
        "as its output so must return java.lang.Iterable of Spark Rows")
    }

    if (outputType.isEmpty && methodReturnType != classOf[Void]) {
      throw new AnalysisException(
        s"Wrong method return type: $methodReturnType; the procedure defines no output columns " +
        "so must be void")
    }
  }

  private def buildArgValues(procedure: Procedure, args: Seq[CallArgument]): Array[Any] = {
    val params = procedure.parameters

    // build a map of declared parameter names to their positions
    val nameToPositionMap = params.map(_.name).zipWithIndex.toMap

    // build a map of parameter names to args
    val nameToArgMap = buildNameToArgMap(params, args, nameToPositionMap)

    // verify all required parameters are provided
    val missingParamNames = params.filter(_.required).collect {
      case param if !nameToArgMap.contains(param.name) => param.name
    }

    if (missingParamNames.nonEmpty) {
      throw new AnalysisException(s"Missing required parameters: ${missingParamNames.mkString("[", ",", "]")}")
    }

    val values = new Array[Any](params.size)

    // convert provided args from internal Spark representation to Scala
    nameToArgMap.foreach { case (name, arg) =>
      val position = nameToPositionMap(name)
      val param = params(position)
      val paramType = param.dataType
      val argType = arg.expr.dataType
      if (paramType != argType) {
        throw new AnalysisException(s"Wrong arg type for ${param.name}: expected $paramType but got $argType")
      }
      values(position) = toScalaValue(arg.expr)
    }

    // assign default values to optional params
    params.foreach {
      case p if !p.required && !nameToArgMap.contains(p.name) =>
        val position = nameToPositionMap(p.name)
        values(position) = p.defaultValue
      case _ =>
    }

    values
  }

  private def buildNameToArgMap(
      params: Seq[ProcedureParameter],
      args: Seq[CallArgument],
      nameToPositionMap: Map[String, Int]): Map[String, CallArgument] = {

    val containsNamedArg = args.exists(_.isInstanceOf[NamedArgument])
    val containsPositionalArg = args.exists(_.isInstanceOf[PositionalArgument])

    if (containsNamedArg && containsPositionalArg) {
      throw new AnalysisException("Named and positional arguments cannot be mixed")
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
      throw new AnalysisException(s"Could not build name to arg map: ${validationErrors.mkString(", ")}")
    }

    namedArgs.map(arg => arg.name -> arg).toMap
  }

  private def buildNameToArgMapUsingPositions(
      args: Seq[CallArgument],
      params: Seq[ProcedureParameter]): Map[String, CallArgument] = {

    if (args.size > params.size) {
      throw new AnalysisException("Too many arguments for procedure")
    }

    args.zipWithIndex.map { case (arg, position) =>
      val param = params(position)
      param.name -> arg
    }.toMap
  }

  private def toScalaValue(expr: Expression): Any = expr match {
    case literal: Literal => CatalystTypeConverters.convertToScala(literal.value, literal.dataType)
    case _ => throw new AnalysisException(s"Cannot convert '$expr' to a Scala literal value")
  }

  private def resolveCatalog(nameParts: Seq[String]): (CatalogPlugin, Identifier) = {
    val catalogManager = SparkSession.active.sessionState.catalogManager

    if (nameParts.length == 1) {
      return (catalogManager.currentCatalog, Identifier.of(catalogManager.currentNamespace, nameParts.head))
    }

    try {
      val catalogName = nameParts.head
      val procedureNameParts = nameParts.tail
      (catalogManager.catalog(catalogName), toIdentifier(procedureNameParts))
    } catch {
      case _: CatalogNotFoundException =>
        (catalogManager.currentCatalog, toIdentifier(nameParts))
    }
  }

  private def toIdentifier(nameParts: Seq[String]): Identifier = {
    Identifier.of(nameParts.init.toArray, nameParts.last)
  }

  implicit class CatalogHelper(plugin: CatalogPlugin) {
    def asProcedureCatalog: ProcedureCatalog = plugin match {
      case procedureCatalog: ProcedureCatalog =>
        procedureCatalog
      case _ =>
        throw new AnalysisException(s"Cannot use catalog ${plugin.name}: not a ProcedureCatalog")
    }
  }
}
