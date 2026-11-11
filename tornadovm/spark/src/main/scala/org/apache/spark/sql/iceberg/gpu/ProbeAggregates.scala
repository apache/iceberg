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
package org.apache.spark.sql.iceberg.gpu

import org.apache.iceberg.tornadovm.join.DeviceDimension
import org.apache.iceberg.tornadovm.join.DeviceProbe
import org.apache.iceberg.tornadovm.join.StarAggregation.ProbeSum
import org.apache.iceberg.tornadovm.DeviceValue
import org.apache.iceberg.tornadovm.spark.GpuAggregates
import org.apache.iceberg.types.Type.TypeID
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Left => _, Right => _, _}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.types._
import scala.collection.mutable

/**
 * Plans probe aggregates: an aggregate over joins of two fact tables that a star cannot express --
 * TPC-DS q72's `catalog_sales` and `inventory`, joined on the item and on the week, a date
 * dimension's attribute, where Spark joins on the item alone and gets a billion rows. The largest
 * table and the dimensions that join it (directly, or through an equality with its columns) are
 * the probe side, a star run on the device; everything else -- the other fact table with its own
 * dimensions and filters, and left outer joins on its columns -- is the build side, a plan Spark
 * runs and collects. Equalities between the two sides make the probe key (a probe-side column or a
 * dimension's integer attribute against a build-side expression), and one comparison of a
 * probe-side column with a build-side expression filters the joined rows. The build side
 * contributes multiplicity only: the aggregates read probe-side columns or count rows.
 */
object ProbeAggregates extends PredicateHelper {
  val Enabled = "spark.iceberg.gpu.aggregate.probe"
  val MaxBuildRows = "spark.iceberg.gpu.aggregate.probe.maxBuildRows"

  // Most keys a fact group key may span by its files' bounds: the groups stay few.
  private val FactKeySpan = 1 << 18

  import GpuAggregateStrategy.traverse
  import GpuAggregateStrategy.substitute

  def spec(agg: Aggregate): Either[String, GpuStarAggregateSpec] =
    if (StarAggregates.failedBefore(agg)) Left("failed on the device before") else plan(agg)

  private def plan(agg: Aggregate): Either[String, GpuStarAggregateSpec] = {
    for {
      upper <- StarAggregates.aboveJoins(agg.child)
      (upperConditions, upperAliases, top) = upper
      _ <- if (upperAliases.isEmpty) Right(()) else Left("projected expressions above the joins")
      outer <- outerJoins(top)
      (outerJoins, innerTree) = outer
      tree <- StarAggregates.joinTree(innerTree)
      (leaves, joinConditions) = tree
      sized <- traverse(leaves) { leaf =>
        leaf.collect { case r: DataSourceV2ScanRelation => r } match {
          case Seq(relation) =>
            GpuAggregateStrategy
              .currentTable(relation)
              .map(t => (leaf, relation, StarAggregates.rowCount(t)))
          case _ => Left(s"a join input that is not one Iceberg scan: ${leaf.nodeName}")
        }
      }
      (factLeaf, factRelation, _) = sized.maxBy(_._3)
      fact <- GpuAggregateStrategy.scanOf(factLeaf)
      (factConditions, factAliases, _) = fact
      table <- GpuAggregateStrategy.currentTable(factRelation)
      factOutput = factLeaf.outputSet
      conditions = upperConditions ++ joinConditions
      equivalent = classes(conditions)
      // The build side's own fact: the next largest table, never a dimension of the probe side
      // (a small one could otherwise attach through its join with the fact).
      others = sized.filter(_._1 ne factLeaf)
      _ <- if (others.nonEmpty) Right(()) else Left("one table")
      // How many equalities join a leaf to the probe fact's columns, directly or through others.
      links = (leaf: LogicalPlan) =>
        conditions.count {
          case EqualTo(a: Attribute, b: Attribute) =>
            (leaf.outputSet.contains(a) && !leaf.outputSet.contains(b) &&
              factAttribute(b, equivalent, factOutput).isDefined) ||
              (leaf.outputSet.contains(b) && !leaf.outputSet.contains(a) &&
                factAttribute(a, equivalent, factOutput).isDefined)
          case _ => false
        }
      (buildFact, _, buildFactRows) = others.maxBy(_._3)
      // Two fact tables: a build side that is itself a dimension is a star's job (TPC-DS q88's
      // count-only stars, slower as probes than on Spark). A table joined to the fact by a
      // composite key (returns by item and order, q64's) is no dimension, whatever its size.
      _ <-
        if (buildFactRows > StarAggregates.dimensionRowLimit || links(buildFact) >= 2) Right(())
        else Left(s"no second fact table (the largest other has $buildFactRows rows)")
      // The probe side's dimensions: small leaves joined by their key to a probe-side column,
      // directly or through equalities.
      attached = others.filter(_._1 ne buildFact).flatMap { case (leaf, _, rows) =>
        // A dimension joins by one key; a table joined to the fact by several equalities (TPC-DS
        // store_returns: customer, item, ticket) is another fact, on the build side.
        if (rows > StarAggregates.dimensionRowLimit || links(leaf) != 1) None
        else
          conditions.collectFirst {
            case EqualTo(k: Attribute, other: Attribute)
                if leaf.outputSet.contains(k) && !leaf.outputSet.contains(other) &&
                  factAttribute(other, equivalent, factOutput).isDefined =>
              (leaf, k, factAttribute(other, equivalent, factOutput).get)
            case EqualTo(other: Attribute, k: Attribute)
                if leaf.outputSet.contains(k) && !leaf.outputSet.contains(other) &&
                  factAttribute(other, equivalent, factOutput).isDefined =>
              (leaf, k, factAttribute(other, equivalent, factOutput).get)
          }
      }
      dimensionLeaves = attached.map(_._1)
      buildLeaves = sized.map(_._1).filter(l => (l ne factLeaf) && !dimensionLeaves.exists(_ eq l))
      _ <- if (buildLeaves.nonEmpty) Right(()) else Left("no build side")
      probeSide = AttributeSet(factOutput.toSeq ++ dimensionLeaves.flatMap(_.output))
      buildSide = AttributeSet(buildLeaves.flatMap(_.output))
      // Each condition: a probe-side filter, a dimension's join or filter, a build-side
      // condition, a key part or the comparison.
      classified <- traverse(conditions) { c =>
        val refs = c.references
        val edge = attached.exists { case (_, k, _) =>
          c match {
            case EqualTo(a: Attribute, b: Attribute) => a == k || b == k
            case _ => false
          }
        }
        if (edge) Right(Edge)
        else if (refs.subsetOf(factOutput)) Right(FactFilter(substitute(c, factAliases)))
        else if (dimensionLeaves.exists(l => refs.subsetOf(l.outputSet))) {
          Right(DimensionFilter(dimensionLeaves.indexWhere(l => refs.subsetOf(l.outputSet)), c))
        } else if (refs.subsetOf(buildSide)) Right(BuildCondition(c))
        else crossCondition(c, probeSide, buildSide)
      }
      factFilter <- GpuAggregateStrategy.icebergFilter(
        classified.collect { case FactFilter(c) => c } ++ factConditions)
      keyParts = classified.collect { case k: KeyPart => k }
      _ <- if (keyParts.nonEmpty) Right(()) else Left("no equality between the two sides")
      comparisons = classified.collect { case c: Compare => c }
      _ <- if (comparisons.size <= 1) Right(()) else Left("more than one comparison")
      _ <- traverse(outerJoins) { case (right, cond) =>
        if (cond.references.subsetOf(AttributeSet(buildSide.toSeq ++ right.output))) Right(())
        else Left(s"a left outer join on probe-side columns: $cond")
      }
      // Aggregates over probe-side columns or rows; group keys over the dimensions' attributes.
      aggregateExpressions = agg.aggregateExpressions
        .flatMap(_.collect { case ae: AggregateExpression => ae })
        .distinct
      outerOutput = AttributeSet(outerJoins.flatMap(_._1.output))
      translated <- {
        val values = mutable.ArrayBuffer[DeviceValue]()
        val buildValues = mutable.ArrayBuffer[Expression]()
        traverse(aggregateExpressions) { ae =>
          if (ae.references.subsetOf(factOutput)) {
            GpuAggregateStrategy
              .deviceAggregate(
                table,
                substitute(ae, factAliases).asInstanceOf[AggregateExpression],
                values,
                nullable = true)
              .map(ae -> _)
          } else if (ae.references.subsetOf(AttributeSet(buildSide.toSeq ++ outerOutput.toSeq))) {
            buildAggregate(ae, values, buildValues).map(ae -> _)
          } else Left(s"aggregate across both sides: $ae")
        }.map { pairs =>
          // The probe's sums follow the fact's values (one, at least, for the row count).
          val facts = math.max(values.size, 1)
          val probeAt = (i: Int) => if (i >= StarAggregates.ProbeSlot) facts + i - StarAggregates.ProbeSlot else i
          (pairs.map { case (ae, kind) => ae -> DeviceAggregate.remap(kind, probeAt) }.toMap, values.toList, buildValues.toList)
        }
      }
      (aggregates, values, buildValueExpressions) = translated
      // COUNT(*) alone still needs a value column for the row count.
      deviceValues = if (values.isEmpty) List(DeviceValue.of(anyNumeric(table, factOutput))) else values
      // A group key from the build side equal to a probe-side dimension's attribute (q72's
      // d1.d_week_seq = d2.d_week_seq) groups by the latter.
      probeEqual = (a: Attribute) =>
        if (probeSide.contains(a)) None
        else
          equivalent.get(a.exprId).flatMap { c =>
            dimensionLeaves.flatMap(_.output).find(o => equivalent.get(o.exprId).contains(c))
          }
      swap = (e: Expression) =>
        e.transformUp { case a: Attribute if probeEqual(a).isDefined => probeEqual(a).get }
      groupExpressions = agg.groupingExpressions.map(swap)
      resultExpressions = agg.aggregateExpressions.map {
        case a: Attribute if probeEqual(a).isDefined => Alias(probeEqual(a).get, a.name)(exprId = a.exprId)
        case named => swap(named).asInstanceOf[NamedExpression]
      }
      keyInputs = AttributeSet(groupExpressions.flatMap(_.references))
      // A fact column as a group key, when its values span few keys by the files' bounds (q64's
      // item): cuDF groups the weighted rows.
      factKeys <- traverse(keyInputs.toSeq.filter(factOutput.contains)) { a =>
        substitute(a, factAliases) match {
          case c: AttributeReference =>
            GpuAggregateStrategy.groupColumn(table, c).flatMap { column =>
              if (GpuAggregates.denseKeys(table, column, 0.0, FactKeySpan.toDouble)) Right(a -> column)
              else Left(s"fact group key $column spans more than $FactKeySpan keys")
            }
          case other => Left(s"a fact group key that is not a column: $other")
        }
      }
      _ <-
        if (keyInputs.forall(a => factOutput.contains(a) || dimensionLeaves.exists(_.outputSet.contains(a))))
          Right(())
        else Left(s"group keys not over the probe side: $keyInputs")
      dimensions <- traverse(attached.indices) { d =>
        val (leaf, key, factColumnAttribute) = attached(d)
        val filters = classified.collect { case DimensionFilter(`d`, c) => c }
        val probeAttributes = keyParts.collect {
          case KeyPart(a: Attribute, _) if leaf.outputSet.contains(a) => a
        }.distinct
        for {
          column <- GpuAggregateStrategy.substitute(factColumnAttribute, factAliases) match {
            case c: AttributeReference => GpuAggregateStrategy.field(table, c).map(_.name())
            case other => Left(s"not a fact column: $other")
          }
          _ <- table.schema().findField(column).`type`().typeId() match {
            case TypeID.INTEGER | TypeID.LONG => Right(())
            case other => Left(s"join on a $other fact column")
          }
          _ <- StarAggregates.denseByStatistics(leaf, key)
        } yield StarDimension(
          if (filters.isEmpty) leaf else Filter(filters.reduce(And), leaf),
          column,
          key,
          keyInputs.toSeq.filter(leaf.outputSet.contains),
          probeAttributes)
      }
      parts <- traverse(keyParts) {
        case KeyPart(a: Attribute, _) if factOutput.contains(a) =>
          GpuAggregateStrategy.substitute(a, factAliases) match {
            case c: AttributeReference =>
              GpuAggregateStrategy.field(table, c).map(f => FactPart(f.name()): ProbePart)
            case other => Left(s"key part not a fact column: $other")
          }
        case KeyPart(a: Attribute, _) =>
          val d = dimensions.indexWhere(_.probeAttributes.contains(a))
          if (d < 0) Left(s"key part $a")
          else Right(AttributePart(d, dimensions(d).probeAttributes.indexOf(a)))
        case other => Left(s"key part $other")
      }
      comparison <- traverse(comparisons) { case Compare(a, how, _) =>
        GpuAggregateStrategy.substitute(a, factAliases) match {
          case c: AttributeReference => GpuAggregateStrategy.field(table, c).map(f => (f.name(), how))
          case other => Left(s"compared column $other")
        }
      }
      build = buildPlan(
        buildLeaves,
        classified.collect { case BuildCondition(c) => c },
        outerJoins,
        keyParts.map(_.build) ++ comparisons.map(_.build) ++ buildValueExpressions)
      deviceKeys = dimensions.count(_.attributes.nonEmpty)
      _ <- if (deviceKeys <= 4) Right(()) else Left(s"$deviceKeys device group keys, at most 4")
    } yield GpuStarAggregateSpec(
      table,
      factFilter,
      dimensions,
      factKeys,
      deviceValues,
      Nil,
      aggregates,
      None,
      groupExpressions,
      resultExpressions,
      Some(StarProbe(build, parts, comparison.headOption, buildValueExpressions.indices.map(ProbeSum.of))))
  }

  /**
   * An aggregate of build-side columns: SUM, AVG, COUNT or a variance of an integral or double expression
   * (a decimal SUM or AVG as its unscaled value, Spark's own rewrite), each a build column the
   * probe sums over every fact row's joined rows.
   */
  private def buildAggregate(
      ae: AggregateExpression,
      values: mutable.ArrayBuffer[DeviceValue],
      buildValues: mutable.ArrayBuffer[Expression]): Either[String, DeviceAggregate] = {
    import org.apache.spark.sql.catalyst.expressions.aggregate._
    if (ae.isDistinct || ae.filter.isDefined) return Left(s"DISTINCT or FILTER: $ae")
    def add(e: Expression): Either[String, Int] = e.dataType match {
      case IntegerType | LongType | DoubleType =>
        buildValues += e
        Right(StarAggregates.ProbeSlot + buildValues.size - 1)
      // A decimal sums as its unscaled value, checked exact when collected, scaled back as the
      // fact's decimal sums are (q64's sum of refunds, credits and charges).
      case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS =>
        buildValues += UnscaledValue(e)
        Right(StarAggregates.ProbeSlot + buildValues.size - 1)
      case other => Left(s"build-side aggregate of $other: $ae")
    }
    ae.aggregateFunction match {
      case Count(Seq(child)) if !child.foldable =>
        add(If(IsNotNull(child), Literal(1L), Literal(0L))).map(CountSum)
      case Sum(child, _) => add(child).map(SumOf)
      case Average(child, _) => add(child).map(AverageOf)
      case m: CentralMomentAgg if m.children.size == 1 && GpuAggregateStrategy.secondMoment(m) =>
        GpuAggregateStrategy.integralColumn(m.children.head) match {
          // Its sums and sums of squares as integral build columns, checked exact when collected.
          case Some(a) =>
            for {
              v <- add(Cast(a, LongType))
              w <- add(Multiply(Cast(a, LongType), Cast(a, LongType)))
            } yield Moment(v, w)
          case None => Left(s"build-side moment of ${m.children.head}")
        }
      case other => Left(s"build-side aggregate $other")
    }
  }

  private sealed trait Role
  private case object Edge extends Role
  private case class FactFilter(c: Expression) extends Role
  private case class DimensionFilter(dimension: Int, c: Expression) extends Role
  private case class BuildCondition(c: Expression) extends Role
  private case class KeyPart(probe: Expression, build: Expression) extends Role
  private case class Compare(probe: Expression, how: DeviceProbe.Comparison, build: Expression)
      extends Role

  /** A probe-side and a build-side expression equal, or one compared with the other. */
  private def crossCondition(
      c: Expression,
      probe: AttributeSet,
      build: AttributeSet): Either[String, Role] = {
    def probeColumn(e: Expression): Option[Attribute] = e match {
      case a: Attribute if probe.contains(a) && integral(a.dataType) => Some(a)
      case Cast(a: Attribute, LongType | IntegerType, _, _) if probe.contains(a) && integral(a.dataType) =>
        Some(a)
      case _ => None
    }
    def buildSide(e: Expression) = e.references.nonEmpty && e.references.subsetOf(build) && integral(e.dataType)
    import DeviceProbe.Comparison._
    def compare(p: Expression, b: Expression, how: DeviceProbe.Comparison): Option[Role] =
      for (a <- probeColumn(p) if buildSide(b)) yield Compare(a, how, b)
    (c match {
      case EqualTo(p, b) if probeColumn(p).isDefined && buildSide(b) => Some(KeyPart(probeColumn(p).get, b))
      case EqualTo(b, p) if probeColumn(p).isDefined && buildSide(b) => Some(KeyPart(probeColumn(p).get, b))
      case LessThan(p, b) => compare(p, b, LESS).orElse(compare(b, p, GREATER))
      case LessThanOrEqual(p, b) => compare(p, b, LESS_EQUAL).orElse(compare(b, p, GREATER_EQUAL))
      case GreaterThan(p, b) => compare(p, b, GREATER).orElse(compare(b, p, LESS))
      case GreaterThanOrEqual(p, b) => compare(p, b, GREATER_EQUAL).orElse(compare(b, p, LESS_EQUAL))
      case _ => None
    }).toRight(s"a condition across the two sides: $c")
  }

  private def integral(t: DataType) = t == IntegerType || t == LongType

  /** Left outer joins above the inner joins, innermost last, and the inner tree below them. */
  private def outerJoins(plan: LogicalPlan)
      : Either[String, (Seq[(LogicalPlan, Expression)], LogicalPlan)] = plan match {
    case Join(left, right, LeftOuter, Some(cond), _) =>
      outerJoins(left).map { case (joins, inner) => (joins :+ (right -> cond), inner) }
    case Project(list, child) if list.forall(_.isInstanceOf[Attribute]) => outerJoins(child)
    case j: Join if j.joinType == Inner => Right((Nil, j))
    case other => Left(s"not a join: ${other.nodeName}")
  }

  /** Equality classes of attributes, by the conditions' attribute equalities. */
  private def classes(conditions: Seq[Expression]): Map[ExprId, Int] = {
    val parent = mutable.HashMap[ExprId, ExprId]()
    def find(x: ExprId): ExprId = {
      val p = parent.getOrElseUpdate(x, x)
      if (p == x) x else { val r = find(p); parent(x) = r; r }
    }
    conditions.foreach {
      case EqualTo(a: Attribute, b: Attribute) => parent(find(a.exprId)) = find(b.exprId)
      case _ =>
    }
    val roots = parent.keys.map(find).toSeq.distinct.zipWithIndex.toMap
    parent.keys.map(k => k -> roots(find(k))).toMap
  }

  /** The fact attribute equal to {@code a}: itself, or one in its equality class. */
  private def factAttribute(
      a: Attribute,
      classes: Map[ExprId, Int],
      factOutput: AttributeSet): Option[Attribute] =
    if (factOutput.contains(a)) Some(a)
    else
      classes.get(a.exprId).flatMap(c => factOutput.toSeq.find(f => classes.get(f.exprId).contains(c)))

  private def anyNumeric(table: org.apache.iceberg.Table, factOutput: AttributeSet): String =
    factOutput.toSeq
      .flatMap(a => Option(table.schema().findField(a.name)))
      .find(f => f.`type`().typeId() == TypeID.INTEGER || f.`type`().typeId() == TypeID.LONG)
      .map(_.name())
      .getOrElse(throw new UnsupportedOperationException("no numeric fact column to count rows by"))

  /** The build side: its leaves joined under its conditions, then the left outer joins. */
  private def buildPlan(
      leaves: Seq[LogicalPlan],
      conditions: Seq[Expression],
      outer: Seq[(LogicalPlan, Expression)],
      outputs: Seq[Expression]): LogicalPlan = {
    val joined = leaves.reduce((l, r) => Join(l, r, Inner, None, JoinHint.NONE))
    val filtered = if (conditions.isEmpty) joined else Filter(conditions.reduce(And), joined)
    val withOuter = outer.foldLeft(filtered: LogicalPlan) { case (plan, (right, cond)) =>
      Join(plan, right, LeftOuter, Some(cond), JoinHint.NONE)
    }
    Project(outputs.zipWithIndex.map { case (e, i) => Alias(e, s"probe_$i")() }, withOuter)
  }

  private[gpu] def maxBuildRows: Long = {
    val env = Option(org.apache.spark.SparkEnv.get).map(_.conf)
    org.apache.spark.sql.internal.SQLConf.get
      .getConfString(MaxBuildRows, env.map(_.get(MaxBuildRows, "20000000")).getOrElse("20000000"))
      .toLong
  }

  /** The build side's rows: each key part's value, then the compared value. */
  /**
   * A collected build side: each key part's values, the compared values, and the summed columns
   * (NaN for null) with each one's largest magnitude and whether it is integral (an integer, or a
   * decimal as its unscaled value), so that the run can bound the sums over the join.
   */
  case class BuildRows(
      parts: Array[Array[Long]],
      compared: Array[Long],
      sums: Array[Array[Double]],
      largest: Array[Double],
      integral: Array[Boolean])

  /**
   * Collected build sides by the query run, their plan and tables' snapshots: a query that
   * repeats a subquery (q64's cross_sales, twice) collects it once. Not across runs: each run of a
   * query collects its build sides again, as Spark computes its joins again.
   */
  private val collected = java.util.Collections.synchronizedMap(
    new java.util.LinkedHashMap[String, java.util.concurrent.CompletableFuture[BuildRows]](8, 0.75f, true) {
      override def removeEldestEntry(
          eldest: java.util.Map.Entry[String, java.util.concurrent.CompletableFuture[BuildRows]]): Boolean =
        size() > 4
    })

  private[gpu] def collect(session: SparkSession, probe: StarProbe, execution: Option[String]): BuildRows =
    execution.map { run =>
      // Within one query run a stage's rows cannot change: the canonical plan identifies the side.
      s"$run|${probe.parts.size}|${probe.comparison.isDefined}|${probe.sums.size}|" +
        probe.build.output.map(_.name).mkString(",") + "|" + StarAggregates.relations(probe.build) + "|" +
        probe.build.canonicalized.toString
    } match {
      case Some(key) =>
        // The two sides of a self-join start together: the second waits for the first's rows.
        val mine = new java.util.concurrent.CompletableFuture[BuildRows]()
        val running = collected.putIfAbsent(key, mine)
        if (running != null) {
          try running.join()
          catch { case e: java.util.concurrent.CompletionException => throw e.getCause }
        } else {
          try {
            val rows = collectNow(session, probe)
            mine.complete(rows)
            rows
          } catch {
            case e: Throwable =>
              collected.remove(key, mine)
              mine.completeExceptionally(e)
              throw e
          }
        }
      case None => collectNow(session, probe)
    }

  private def collectNow(session: SparkSession, probe: StarProbe): BuildRows = {
    val rows = Dataset.ofRows(session, probe.build).queryExecution.executedPlan.executeCollect()
    if (rows.length > maxBuildRows) {
      throw new UnsupportedOperationException(s"a build side of ${rows.length} rows")
    }
    val n = probe.parts.size
    val types = probe.build.output.map(_.dataType)
    def long(row: InternalRow, i: Int) =
      if (types(i) == IntegerType) row.getInt(i).toLong else row.getLong(i)
    // An inner equality never joins a null; a null compared value joins nothing either. Summed
    // build values may be null (NaN, skipped on the device).
    val compared = if (probe.comparison.isEmpty) 0 else 1
    val live = rows.filter(r => (0 until n + compared).forall(i => !r.isNullAt(i)))
    val sums = Array.tabulate(probe.sums.size) { k =>
      val i = n + compared + k
      live.map { r =>
        if (r.isNullAt(i)) Double.NaN
        else
          types(i) match {
            case IntegerType => r.getInt(i).toDouble
            case LongType => r.getLong(i).toDouble
            case _ => r.getDouble(i)
          }
      }
    }
    // Whether the sums over the join stay exact is decided by the run, which knows the join's
    // multiplicity (GpuStarAggregateSpec.requireExact).
    BuildRows(
      Array.tabulate(n)(p => live.map(long(_, p))),
      if (compared == 0) new Array[Long](live.length) else live.map(long(_, n)),
      sums,
      sums.map(column => column.foldLeft(0.0)((m, v) => if (v.isNaN) m else math.max(m, math.abs(v)))),
      sums.indices.map(k => types(n + compared + k) != DoubleType).toArray)
  }

  /**
   * The device's dimensions, with each probe attribute's codes, and the probe: each part's value
   * less its build-side minimum, times the product of the later parts' ranges.
   */
  private[gpu] def prepare(
      probe: StarProbe,
      rows: BuildRows,
      dimensions: Seq[StarDimension],
      built: Seq[StarAggregates.Built]): (Seq[DeviceDimension], DeviceProbe) = {
    val n = probe.parts.size
    val offsets = rows.parts.map(v => if (v.isEmpty) 0L else v.min)
    val ranges = rows.parts.zip(offsets).map { case (v, o) => if (v.isEmpty) 1L else v.max - o + 1 }
    if (offsets.exists(o => o < Int.MinValue || o > Int.MaxValue) || ranges.exists(_ > Int.MaxValue)) {
      throw new UnsupportedOperationException("probe key parts out of int range")
    }
    val multipliers = new Array[Long](n)
    var weight = 1L
    for (p <- n - 1 to 0 by -1) {
      multipliers(p) = weight
      if (ranges(p) > Long.MaxValue / 4 / weight) {
        throw new UnsupportedOperationException("probe key space too large")
      }
      weight *= ranges(p)
    }
    val keys = new Array[Long](rows.compared.length)
    for (p <- 0 until n) {
      val part = rows.parts(p)
      var r = 0
      while (r < keys.length) {
        keys(r) += (part(r) - offsets(p)) * multipliers(p)
        r += 1
      }
    }
    var withCodes = built.map(_._1).toArray
    var deviceProbe = DeviceProbe.of(keys, rows.compared, java.util.Arrays.asList(rows.sums: _*))
    probe.parts.zipWithIndex.foreach {
      case (FactPart(column), p) =>
        deviceProbe = deviceProbe.keyColumn(column, multipliers(p), offsets(p).toInt, ranges(p).toInt)
      case (AttributePart(d, j), p) =>
        val raw = built(d)._3(j)
        val codes = raw.map { v =>
          if (v == Long.MinValue || v < offsets(p) || v - offsets(p) >= ranges(p)) ranges(p).toInt
          else (v - offsets(p)).toInt
        }
        val index = (if (dimensions(d).attributes.nonEmpty) 1 else 0) + countAttributes(withCodes(d), dimensions(d))
        withCodes(d) = withCodes(d).withAttribute(codes)
        deviceProbe = deviceProbe.keyAttribute(d, index, multipliers(p), 0, ranges(p).toInt)
    }
    probe.comparison.foreach { case (column, how) => deviceProbe = deviceProbe.where(column, how) }
    if (probe.outer) {
      deviceProbe = deviceProbe.leftOuter()
    }
    (withCodes.toSeq, deviceProbe)
  }

  // Probe attributes appended so far to a dimension, after its group tuple.
  private def countAttributes(d: DeviceDimension, star: StarDimension): Int =
    d.attributeCount() - (if (star.attributes.nonEmpty) 1 else 0)
}
