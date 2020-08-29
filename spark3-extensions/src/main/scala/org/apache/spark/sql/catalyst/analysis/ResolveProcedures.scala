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

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Call, CallArgument, CallStatement, LogicalPlan, NamedArgument, PositionalArgument}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogNotFoundException, CatalogPlugin, Identifier, Procedure, ProcedureCatalog, ProcedureParameter}
import scala.collection.{mutable, Seq}

// TODO: case sensitivity?
object ResolveProcedures extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case CallStatement(nameParts, args) =>
      val (catalog, ident) = resolveCatalog(nameParts)

      val procedure = catalog.asProcedureCatalog.loadProcedure(ident)
      validateParams(procedure)
      validateMethodHandle(procedure)

      val argValues = prepareArgValues(procedure, args)
      Call(procedure, argValues)
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

  private def validateParams(procedure: Procedure): Unit = {
    val params = procedure.parameters

    // should not be any duplicate param names
    params.groupBy(_.name)
      .find { case (_, params) => params.length > 1 }
      .foreach { case (paramName, _) =>
        throw new AnalysisException(s"Duplicate parameter name: '$paramName'");
      }

    // optional params should be at the end
    params.sliding(2).foreach { case Array(previousParam, currentParam) =>
      if (previousParam.required && !currentParam.required) {
        throw new AnalysisException("Optional parameters must be after required ones")
      }
    }
  }

  private def validateMethodHandle(procedure: Procedure): Unit = {
    val params = procedure.parameters
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

    if (procedure.outputType.nonEmpty && methodReturnType != classOf[java.lang.Iterable[Row]]) {
      throw new AnalysisException(
        s"Wrong method return type: $methodReturnType; the procedure defines ${procedure.outputType} " +
        s"as its output so must return java.lang.Iterable of Spark Rows")
    }

    if (procedure.outputType.isEmpty && methodReturnType != classOf[Void]) {
      throw new AnalysisException(
        s"Wrong method return type: $methodReturnType; the procedure defines no output columns " +
        s"so must be void")
    }
  }

  private def prepareArgValues(procedure: Procedure, args: Seq[CallArgument]): Array[Any] = {
    // build a map of declared parameter names to their positions
    val params = procedure.parameters
    val nameToPositionMap = params.map(_.name).zipWithIndex.toMap

    // verify named and positional args are not mixed
    val containsNamedArg = args.exists(_.isInstanceOf[NamedArgument])
    val containsPositionalArg = args.exists(_.isInstanceOf[PositionalArgument])
    if (containsNamedArg && containsPositionalArg) {
      throw new AnalysisException("Named and positional arguments cannot be mixed")
    }

    // build a map of parameter names to args
    val nameToArgMap = if (containsNamedArg) {
      buildNameToArgMapUsingNames(args, nameToPositionMap)
    } else {
      buildNameToArgMapUsingPositions(args, params)
    }

    // verify all required parameters are provided
    params.filter(_.required)
      .find(param => !nameToArgMap.contains(param.name))
      .foreach { missingArg =>
        throw new AnalysisException(s"Required procedure argument '${missingArg.name}' is missing")
      }

    val values = new Array[Any](params.size)

    // convert provided args from internal Spark representation to Scala
    nameToArgMap.foreach { case (name, arg) =>
      val position = nameToPositionMap(name)
      val param = params(position)
      val paramType = param.dataType
      val argType = arg.expr.dataType
      if (paramType != argType) {
        throw new AnalysisException(s"Wrong arg type for '${param.name}': expected $paramType but got $argType")
      }
      values(position) = toScalaValue(arg.expr)
    }

    // assign default values for optional params
    params.foreach {
      case p if !p.required && !nameToArgMap.contains(p.name) =>
        val position = nameToPositionMap(p.name)
        values(position) = p.defaultValue
      case _ =>
    }

    values
  }

  private def buildNameToArgMapUsingNames(
      args: Seq[CallArgument],
      nameToPositionMap: Map[String, Int]): Map[String, CallArgument] = {

    val nameToArgMap = mutable.LinkedHashMap.empty[String, CallArgument]
    args.foreach { case arg: NamedArgument =>
      val name = arg.name

      if (nameToArgMap.contains(name)) {
        throw new AnalysisException(s"Duplicate procedure argument: '$name'")
      }

      if (!nameToPositionMap.contains(name)) {
        throw new AnalysisException(s"Unknown argument name: '$name'")
      }

      nameToArgMap.put(name, arg)
    }
    nameToArgMap.toMap
  }

  private def buildNameToArgMapUsingPositions(
      args: Seq[CallArgument],
      params: Seq[ProcedureParameter]): Map[String, CallArgument] = {

    if (args.size > params.size) {
      throw new AnalysisException("Too many arguments for procedure")
    }

    val nameToArgMap = mutable.LinkedHashMap.empty[String, CallArgument]
    args.zipWithIndex.foreach { case (arg, position) =>
      val param = params(position)
      nameToArgMap.put(param.name, arg)
    }
    nameToArgMap.toMap
  }

  private def toScalaValue(expr: Expression): Any = expr match {
    case literal: Literal => CatalystTypeConverters.convertToScala(literal.value, literal.dataType)
    case _ => throw new AnalysisException(s"Cannot convert '$expr' to a Scala literal value")
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
