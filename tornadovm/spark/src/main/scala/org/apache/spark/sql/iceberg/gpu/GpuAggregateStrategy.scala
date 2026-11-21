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

import org.apache.iceberg.expressions.{Expression => IcebergExpression}
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.spark.SparkV2Filters
import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.tornadovm.DeviceValue
import org.apache.iceberg.tornadovm.spark.GpuAggregates
import org.apache.iceberg.types.Type.TypeID
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.classic.Strategy
import org.apache.spark.sql.catalyst.expressions.{Left => _, Right => _, _}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

/** `spark.sql.extensions=org.apache.spark.sql.iceberg.gpu.GpuAggregateExtensions` */
class GpuAggregateExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy(_ => GpuAggregateStrategy)
  }
}

/** What one aggregate function of the query becomes: device sums and the row count. */
sealed trait DeviceAggregate
case object RowCount extends DeviceAggregate
case class SumOf(value: Int) extends DeviceAggregate
case class AverageOf(value: Int) extends DeviceAggregate
case class CountOf(value: Int) extends DeviceAggregate

/** SUM(COALESCE(value, 0)): the value's sum, NULL only when the group has no row. */
case class SumAll(value: Int) extends DeviceAggregate

/**
 * A variance or standard deviation (Spark's second central moment) of an integral value: its sum
 * (value {@code value}), its sum of squares (value {@code squares}) and its non-null count.
 */
case class Moment(value: Int, squares: Int) extends DeviceAggregate

/**
 * SUM and AVG of {@code e / k} with each row's quotient exact (TPC-DS q4's `(...)/2`): the device
 * sums {@code e}'s unscaled value (value {@code value}); the result's unscaled sum is that times
 * {@code multiplier} = 10^(result scale - e's scale) / k.
 */
case class ScaledSum(value: Int, multiplier: Long) extends DeviceAggregate
case class ScaledAverage(value: Int, multiplier: Long) extends DeviceAggregate

/**
 * SUM of a signed sum of terms across a left outer join (TPC-DS q80's `net_profit -
 * COALESCE(net_loss, 0)`): each term a device value (a fact column, or a build-side column summed
 * over the joined rows), the result their exact signed sum; NULL when the group has no row, or
 * when {@code nullsFrom}'s value had no non-null row.
 */
case class LinearSum(terms: Seq[(Long, Int)], nullsFrom: Option[Int]) extends DeviceAggregate

/** COUNT of a probe's build-side column: the sum of its 0/1 non-null indicator (value {@code value}). */
case class CountSum(value: Int) extends DeviceAggregate

/**
 * SUM (or, with {@code all}, SUM of COALESCE(v, 0)) and AVG of an integral value whose sum would
 * pass 2^53: the device sums its three parts (values {@code parts}: high, middle, low, see
 * DeviceValue.part), each exact in doubles, and the host recombines them exactly.
 */
case class SplitSum(parts: Seq[Int], all: Boolean) extends DeviceAggregate
case class SplitAverage(parts: Seq[Int]) extends DeviceAggregate

object DeviceAggregate {

  /** The aggregate with each value index mapped by {@code f}. */
  def remap(kind: DeviceAggregate, f: Int => Int): DeviceAggregate = kind match {
    case RowCount => RowCount
    case SumOf(v) => SumOf(f(v))
    case AverageOf(v) => AverageOf(f(v))
    case CountOf(v) => CountOf(if (v < 0) v else f(v))
    case SumAll(v) => SumAll(f(v))
    case Moment(v, w) => Moment(f(v), f(w))
    case ScaledSum(v, m) => ScaledSum(f(v), m)
    case ScaledAverage(v, m) => ScaledAverage(f(v), m)
    case LinearSum(terms, nulls) => LinearSum(terms.map(t => (t._1, f(t._2))), nulls.map(f))
    case CountSum(v) => CountSum(f(v))
    case SplitSum(parts, all) => SplitSum(parts.map(f), all)
    case SplitAverage(parts) => SplitAverage(parts.map(f))
  }
}

/**
 * Plans a grouped aggregate over an Iceberg table -- SUM, AVG and COUNT of products of double
 * columns and `1 - c` / `1 + c` factors, grouped by up to four integer, date, timestamp or short
 * string columns, under a filter the device can apply -- as one device scan
 * ([[GpuAggregateExec]]): merge-on-read deletes, the filter, the arithmetic and the grouping all
 * on the GPU, only the groups back on the host. Anything else plans as Spark would.
 *
 * Local mode only: the device scan runs in the driver, which there is the executor too.
 */
object GpuAggregateStrategy extends Strategy with PredicateHelper with Logging {
  val Enabled = "spark.iceberg.gpu.enabled"
  val AggregateEnabled = "spark.iceberg.gpu.aggregate.enabled"

  /**
   * Aggregates of columns that may hold nulls (SUM and AVG skip them, COUNT(column) counts the
   * rest), off by default: measured on TPC-DS SF10, they turn many small scalar subqueries (q9's
   * fifteen) into one device scan each, slower than Spark's CPU (q9 1.5 s to 4.3 s). They pay where
   * one device aggregate replaces a large CPU one, as with equality deletes.
   */
  val NullableValues = "spark.iceberg.gpu.aggregate.nullable"

  /**
   * Star aggregates ([[StarAggregates]]): an aggregate over inner joins of one Iceberg fact table
   * with small dimension tables, the joins done on the device. Off by default until measured.
   */
  val StarEnabled = "spark.iceberg.gpu.aggregate.star"

  /** Read options that pick another state of the table than its current one. */
  private val TimeTravel = Set(
    "snapshot-id",
    "as-of-timestamp",
    "branch",
    "tag",
    "start-snapshot-id",
    "end-snapshot-id",
    "start-timestamp",
    "end-timestamp")

  /** Set while a declined aggregate plans on the CPU, so that it does not come back here. */
  private[gpu] val planningFallback = new ThreadLocal[Boolean] {
    override def initialValue(): Boolean = false
  }

  private[gpu] def flag(key: String, default: String): Boolean = {
    val env = Option(SparkEnv.get).map(_.conf)
    SQLConf.get.getConfString(key, env.map(_.get(key, default)).getOrElse(default)).toBoolean
  }

  private def enabled: Boolean =
    flag(Enabled, "false") && flag(AggregateEnabled, "true") &&
      Option(SparkEnv.get).exists(_.conf.get("spark.master", "").startsWith("local"))

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case agg: Aggregate if !planningFallback.get() && enabled =>
      planDevice(agg) match {
        case Right(spec) => Seq(GpuAggregateExec(agg.output, spec, agg))
        case Left(reasons) =>
          logInfo(s"GPU aggregate declined: $reasons")
          Nil
      }
    case _ => Nil
  }

  /**
   * The aggregate as the first planner that takes it plans it for the device -- over one scan,
   * a star of joins, a probe join, a union of those -- or why each declined. Whatever planning
   * cannot handle, even by failing, plans as Spark would.
   */
  private[gpu] def planDevice(agg: Aggregate): Either[String, DeviceAggregateSpec] = {
    val joins =
      (if (flag(StarEnabled, "false")) Seq("as a star aggregate: " -> (StarAggregates.spec _)) else Nil) ++
        (if (flag(ProbeAggregates.Enabled, "false")) Seq("as a probe aggregate: " -> (ProbeAggregates.spec _))
         else Nil)
    val unions =
      if (joins.isEmpty || !flag(UnionAggregates.Enabled, "true")) Nil
      else Seq("as a union aggregate: " -> ((a: Aggregate) => UnionAggregates.spec(a, planDevice)))
    val planners: Seq[(String, Aggregate => Either[String, DeviceAggregateSpec])] =
      Seq("" -> (deviceSpec _)) ++ joins ++ unions
    val reasons = Seq.newBuilder[String]
    val planned = planners.iterator.map { case (name, planner) =>
      scala.util.Try(planner(agg)).toEither.left.map(e => s"planning failed: $e").joinRight.left.map { reason =>
        reasons += name + reason
        reason
      }
    }.find(_.isRight)
    planned.getOrElse(Left(reasons.result().mkString("; ")))
  }

  private def deviceSpec(agg: Aggregate): Either[String, GpuAggregateSpec] = {
    for {
      source <- scanOf(agg.child)
      (conditions, aliases, relation) = source
      groupExpressions = agg.groupingExpressions.map(substitute(_, aliases))
      resultExpressions = agg.aggregateExpressions.map {
        // An output that is a projected alias keeps its name and id, now over what it stands for.
        case a: Attribute if aliases.contains(a.exprId) =>
          Alias(aliases(a.exprId), a.name)(exprId = a.exprId)
        case named => substitute(named, aliases).asInstanceOf[NamedExpression]
      }
      table <- currentTable(relation)
      filter <- icebergFilter(conditions)
      bucket <- groupExpressions match {
        case Seq(BucketOf(buckets, a)) =>
          field(table, a).flatMap { f =>
            if (f.isRequired && f.`type`().typeId() == TypeID.LONG) Right(Some((f.name(), buckets)))
            else Left(s"bucket of ${a.name}: not a required bigint column")
          }
        case _ => Right(None)
      }
      groups <-
        if (bucket.isDefined) Right(Nil)
        else
          traverse(groupExpressions) {
            case a: AttributeReference => groupColumn(table, a)
            case other => Left(s"group expression is not a column: $other")
          }
      _ <- if (groups.size <= 4) Right(()) else Left("more than 4 group columns")
      // The groups the integer keys can make, by their files' bounds: a grouping nearly as fine as
      // the rows (TPC-H q18's per-order sums) copies every group back and merges them on the
      // driver, slower than Spark's own aggregate.
      groupBound = groupExpressions.collect { case a: AttributeReference => GpuAggregates.keyRange(table, a.name) }
        .filterNot(_.isNaN).product
      _ <-
        if (bucket.isDefined || groupBound <= StarAggregates.maxGroups) Right(())
        else Left(f"up to $groupBound%.0f groups by the keys' bounds, over ${StarAggregates.maxGroups}%.0f")
      aggregates = resultExpressions
        .flatMap(_.collect { case ae: AggregateExpression => ae })
        .distinct
      translated <- {
        val values = scala.collection.mutable.ArrayBuffer[DeviceValue]()
        traverse(aggregates)(ae => deviceAggregate(table, ae, values, nullable = false).map(ae -> _))
          .map(pairs => (pairs, values.toList))
      }
      (pairs, values) = translated
      _ <- if (values.nonEmpty) Right(()) else Left("no SUM or AVG")
      sumColumns <-
        if (bucket.isEmpty) Right(Nil)
        else
          traverse(pairs.collect { case (ae, kind) if kind != RowCount => ae }) { ae =>
            ae.aggregateFunction.children match {
              // The bucket kernel sums plain columns and counts rows, not non-null values.
              case Seq(a: AttributeReference) if a.dataType == DoubleType =>
                nullFree(table, a).map(_ => a.name)
              case _ => Left(s"bucket aggregates sum plain double columns only: $ae")
            }
          }
    } yield GpuAggregateSpec(
      table,
      filter,
      groupExpressions,
      groups,
      bucket,
      sumColumns,
      values,
      pairs.toMap,
      resultExpressions)
  }

  /** The table a scan reads, if the scan reads its current state. */
  private[gpu] def currentTable(
      relation: DataSourceV2ScanRelation): Either[String, org.apache.iceberg.Table] =
    relation.relation.table match {
      // Spark 4.2's SparkTable pins the snapshot it was loaded at, also when not time travelling:
      // the scan reads the current state when that snapshot is the table's current one.
      case t: SparkTable
          if (t.snapshotId() == null ||
            Option(t.table().currentSnapshot()).exists(_.snapshotId() == t.snapshotId())) &&
            (t.branch() == null || t.branch() == "main") &&
            !relation.relation.options.keySet().asScala.exists(TimeTravel.contains) =>
        Right(t.table())
      case other => Left(s"not the current state of an Iceberg table: $other")
    }

  /** `bucket(n, column)`, Iceberg's bucket transform of a bigint column, as Spark calls it. */
  private object BucketOf {
    def unapply(e: Expression): Option[(Int, AttributeReference)] = e match {
      case s: org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
          if s.staticObject == classOf[org.apache.iceberg.spark.functions.BucketFunction.BucketLong] =>
        arguments(s.arguments)
      case a: ApplyFunctionExpression
          if a.function.isInstanceOf[org.apache.iceberg.spark.functions.BucketFunction.BucketLong] =>
        arguments(a.children)
      case _ => None
    }

    private def arguments(args: Seq[Expression]): Option[(Int, AttributeReference)] = args match {
      case Seq(Literal(n: Int, IntegerType), a: AttributeReference) => Some((n, a))
      case _ => None
    }
  }

  /**
   * Projections and filters down to an Iceberg scan: the filter's conditions, and what each
   * projected alias stands for, in terms of the scan's columns.
   */
  private[gpu] def scanOf(plan: LogicalPlan)
      : Either[String, (Seq[Expression], Map[ExprId, Expression], DataSourceV2ScanRelation)] =
    plan match {
      case Project(list, child) if list.forall(e => e.deterministic) =>
        scanOf(child).flatMap { case (conditions, aliases, relation) =>
          traverse(list) {
            case _: Attribute => Right(None)
            case alias @ Alias(e, _) if !e.exists(_.isInstanceOf[AggregateExpression]) =>
              Right(Some(alias.exprId -> substitute(e, aliases)))
            case other => Left(s"projection: $other")
          }.map(added => (conditions, aliases ++ added.flatten, relation))
        }
      case Filter(condition, child) =>
        scanOf(child).map { case (conditions, aliases, relation) =>
          (splitConjunctivePredicates(substitute(condition, aliases)) ++ conditions, aliases, relation)
        }
      case relation: DataSourceV2ScanRelation => Right((Nil, Map.empty, relation))
      case other => Left(s"not a filtered scan: ${other.nodeName}")
    }

  private[gpu] def substitute(e: Expression, aliases: Map[ExprId, Expression]): Expression =
    if (aliases.isEmpty) e
    else e.transformUp { case a: AttributeReference if aliases.contains(a.exprId) => aliases(a.exprId) }

  /** Every condition as an Iceberg expression, or a refusal: the device must apply all. */
  private[gpu] def icebergFilter(conditions: Seq[Expression]): Either[String, IcebergExpression] =
    traverse(conditions) { condition =>
      DataSourceV2Strategy
        .translateFilterV2(condition)
        .flatMap(predicate => Option(SparkV2Filters.convert(predicate)))
        .toRight(s"filter not translatable: $condition")
    }.map(_.foldLeft(Expressions.alwaysTrue(): IcebergExpression)(Expressions.and))

  private[gpu] def field(table: org.apache.iceberg.Table, a: Attribute) =
    Option(table.schema().findField(a.name)).toRight(s"not a table column: ${a.name}")

  private[gpu] def groupColumn(table: org.apache.iceberg.Table, a: Attribute): Either[String, String] =
    field(table, a).flatMap { f =>
      f.`type`().typeId() match {
        case TypeID.INTEGER | TypeID.LONG | TypeID.DATE | TypeID.TIMESTAMP | TypeID.STRING =>
          Right(f.name())
        case TypeID.DECIMAL if a.dataType.asInstanceOf[DecimalType].precision <= 18 =>
          Right(f.name())
        case other => Left(s"cannot group by $other")
      }
    }

  /**
   * {@code nullable}: whether values may hold nulls regardless of [[NullableValues]] -- a star
   * aggregate's, whose single device scan replaces joins, which pays for the count columns.
   */
  private[gpu] def deviceAggregate(
      table: org.apache.iceberg.Table,
      ae: AggregateExpression,
      values: scala.collection.mutable.ArrayBuffer[DeviceValue],
      nullable: Boolean): Either[String, DeviceAggregate] = {
    val nulls = nullable || flag(NullableValues, "false")
    if (ae.isDistinct || ae.filter.isDefined) {
      return Left(s"DISTINCT or FILTER: $ae")
    }
    def add(value: DeviceValue): Int = {
      values += value
      values.size - 1
    }
    def valueIndex(child: Expression): Either[String, Int] =
      deviceValue(table, child, nulls).map(add)
    // An integral sum exact in doubles as it is, or else -- every row's value below 2^53, the sum
    // past it -- exact once split into three parts (TPC-H q1's revenue over 60M rows).
    def exactOrSplit(child: Expression, n: Boolean)(
        whole: Int => DeviceAggregate,
        split: Seq[Int] => DeviceAggregate): Either[String, DeviceAggregate] =
      exactColumn(table, child, n) match {
        case Right(v) => Right(whole(add(v)))
        case Left(reason) if reason.contains("may not be exact") =>
          GpuAggregates.splitting(() => exactColumn(table, child, n)) match {
            case Right(v) if v.part() == DeviceValue.WHOLE =>
              Right(split(Seq(DeviceValue.HIGH, DeviceValue.MIDDLE, DeviceValue.LOW).map(p => add(v.part(p)))))
            case _ => Left(reason)
          }
        case left => left.map(_ => RowCount)
      }
    ae.aggregateFunction match {
      case Count(Seq(a: AttributeReference))
          if a.nullable && nulls && nullFree(table, a).isLeft =>
        // COUNT of a column that may hold nulls: the rows where it is not null. Any numeric
        // column counts, whatever its sum would be.
        field(table, a).flatMap { f =>
          f.`type`().typeId() match {
            case TypeID.INTEGER | TypeID.LONG | TypeID.DOUBLE | TypeID.DECIMAL =>
              Right(CountOf(add(DeviceValue.of(f.name()))))
            case other => Left(s"COUNT of a $other column")
          }
        }
      case Count(children) =>
        traverse(children) {
          case l: Literal if l.value != null => Right(())
          case a: AttributeReference => nullFree(table, a)
          case other => Left(s"COUNT of $other")
        }.map(_ => RowCount)
      case m: CentralMomentAgg if m.children.size == 1 && secondMoment(m) =>
        // Of an integer (as a double): sums of it and of its square, exact in doubles.
        integralColumn(m.children.head) match {
          case Some(a) =>
            for {
              f <- field(table, a)
              _ <- valueMayBeNull(table, a, nulls)
              _ <-
                if (GpuAggregates.exactProduct(table, List(f.name(), f.name()).asJava)) Right(())
                else Left(s"squares of ${a.name} may not sum exactly in doubles")
            } yield Moment(add(DeviceValue.of(f.name())), add(DeviceValue.of(f.name()).times(f.name())))
          case None => Left(s"moment of $m")
        }
      case Sum(ExactQuotient(e, multiplier), _) =>
        exactColumn(table, UnscaledValue(e), nulls).map(add).map(ScaledSum(_, multiplier))
      case Average(ExactQuotient(e, multiplier), _) =>
        exactColumn(table, UnscaledValue(e), nulls).map(add).map(ScaledAverage(_, multiplier))
      case s: Sum if s.child.dataType == DoubleType => valueIndex(s.child).map(SumOf)
      case a: Average if a.child.dataType == DoubleType => valueIndex(a.child).map(AverageOf)
      case Sum(Coalesce(Seq(v, zero: Literal)), _)
          if zero.value != null && zero.value.toString.toDouble == 0.0 =>
        exactOrSplit(v, true)(SumAll, SplitSum(_, all = true))
      case s: Sum => exactOrSplit(s.child, nulls)(SumOf, SplitSum(_, all = false))
      case a: Average => exactOrSplit(a.child, nulls)(AverageOf, SplitAverage)
      case other => Left(s"aggregate not supported: $other")
    }
  }

  /**
   * An int, bigint or decimal (up to 18 digits, as its unscaled value) column whose sum the
   * device's doubles hold exactly, by the files' bounds: Spark sums it as a bigint or a decimal,
   * exactly too.
   */
  /** An exact device value (a decimal as its unscaled value) of a fact expression. */
  private[gpu] def exactValue(
      table: org.apache.iceberg.Table,
      e: Expression): Either[String, DeviceValue] = exactColumn(table, e, nulls = true)

  private def exactColumn(
      table: org.apache.iceberg.Table,
      child: Expression,
      nulls: Boolean): Either[String, DeviceValue] = child match {
    // Spark's DecimalAggregates has already turned SUM and AVG of a short decimal into ones of its
    // unscaled value: what the device sums.
    case UnscaledValue(a: AttributeReference) if exactType(a.dataType) => exactColumn(table, a, nulls)
    // A sum or difference of decimals of the result's scale (TPC-DS q11's ext_list_price -
    // ext_discount_amt): the unscaled values add up the same way.
    case UnscaledValue(Cast(a: AttributeReference, to: DecimalType, _, _)) if (a.dataType match {
          case from: DecimalType => from.scale == to.scale && from.precision <= to.precision
          case _ => false
        }) =>
      exactColumn(table, a, nulls)
    case UnscaledValue(e @ (_: Add | _: Subtract)) =>
      val scale = e.dataType.asInstanceOf[DecimalType].scale
      exactTerms(table, e, nulls, {
        case a: AttributeReference => a.dataType match {
          case d: DecimalType => d.scale == scale
          case _ => false
        }
        case _ => false
      })
    case e @ (_: Add | _: Subtract) if e.dataType == LongType || e.dataType == IntegerType =>
      exactTerms(table, e, nulls, {
        case a: AttributeReference => a.dataType == LongType || a.dataType == IntegerType
        case _ => false
      })
    // A decimal widened at its scale (a CASE's common type, say) keeps its unscaled value.
    case Cast(a: AttributeReference, to: DecimalType, _, _) if (a.dataType match {
          case from: DecimalType => from.scale == to.scale && from.precision <= to.precision
          case _ => false
        }) =>
      exactColumn(table, a, nulls)
    case a: AttributeReference if exactType(a.dataType) =>
      for {
        f <- field(table, a)
        _ <- valueMayBeNull(table, a, nulls)
        _ <-
          if (GpuAggregates.exactSum(table, a.name)) Right(())
          else Left(s"sum of ${a.name} may not be exact in doubles")
      } yield DeviceValue.of(f.name())
    // A product of such columns (an int or bigint cast to a decimal of scale 0 is its value), its
    // unscaled value the product of theirs: Spark multiplies decimals exactly too.
    case m: Multiply =>
      // A factor: a column, or 1 plus or minus a decimal column (TPC-H's price * (1 - discount)),
      // which in unscaled integers is 10^scale +- the column: offset and scale of the factor.
      def isOne(e: Expression): Boolean = e.foldable && (e.eval() match {
        case d: org.apache.spark.sql.types.Decimal => d.compare(org.apache.spark.sql.types.Decimal(1)) == 0
        case i: java.lang.Integer => i == 1
        case l: java.lang.Long => l == 1L
        case _ => false
      })
      def affine(a: AttributeReference, sign: Double): Either[String, Seq[(AttributeReference, Double, Double)]] =
        a.dataType match {
          case d: DecimalType => Right(Seq((a, math.pow(10, d.scale), sign)))
          case IntegerType | LongType => Right(Seq((a, 1.0, sign)))
          case other => Left(s"factor 1 +- $other")
        }
      def factors(e: Expression): Either[String, Seq[(AttributeReference, Double, Double)]] = e match {
        case Multiply(l, r, _) => for (a <- factors(l); b <- factors(r)) yield a ++ b
        case Cast(a: AttributeReference, d: DecimalType, _, _)
            if d.scale == 0 && (a.dataType == IntegerType || a.dataType == LongType) =>
          Right(Seq((a, 0.0, 1.0)))
        case Cast(a: AttributeReference, to: DecimalType, _, _) if (a.dataType match {
              case from: DecimalType => from.scale == to.scale && from.precision <= to.precision
              case _ => false
            }) =>
          Right(Seq((a, 0.0, 1.0)))
        case a: AttributeReference if exactType(a.dataType) => Right(Seq((a, 0.0, 1.0)))
        case Subtract(one, a: AttributeReference, _) if isOne(one) && exactType(a.dataType) => affine(a, -1)
        case Add(one, a: AttributeReference, _) if isOne(one) && exactType(a.dataType) => affine(a, 1)
        case Add(a: AttributeReference, one, _) if isOne(one) && exactType(a.dataType) => affine(a, 1)
        case other => Left(s"factor $other")
      }
      for {
        columns <- factors(m)
        _ <- if (columns.size <= DeviceValue.MAX_FACTORS) Right(()) else Left(s"factors of $m")
        names <- traverse(columns)(c => field(table, c._1).map(_.name()))
        _ <- traverse(columns)(c => valueMayBeNull(table, c._1, nulls))
        value = names.zip(columns).tail.foldLeft(
          if (columns.head._2 == 0) DeviceValue.of(names.head)
          else DeviceValue.ofAffine(names.head, columns.head._2, columns.head._3)) {
          case (v, (n, (_, 0.0, _))) => v.times(n)
          case (v, (n, (_, offset, scale))) => v.timesAffine(n, offset, scale)
        }
        _ <-
          if (columns.forall(_._2 == 0)) {
            if (GpuAggregates.exactProduct(table, names.asJava)) Right(())
            else Left(s"sum of $m may not be exact in doubles")
          } else if (GpuAggregates.exactOverJoin(table, java.util.Collections.singletonList(value), 1L)) Right(())
          else Left(s"sum of $m may not be exact in doubles")
      } yield value
    case other => Left(s"aggregate of $other")
  }

  /**
   * {@code e / k}, a decimal over a positive integer literal, where every row's quotient is exact
   * at the result's scale (10^(its scale - e's scale) divisible by k): the dividend and the
   * multiplier from its unscaled sum to the quotients' unscaled sum.
   */
  private object ExactQuotient {
    def unapply(e: Expression): Option[(Expression, Long)] = e match {
      case Divide(dividend, Literal(k, _), _) =>
        (dividend.dataType, e.dataType, Option(k).map(v => new java.math.BigDecimal(v.toString))) match {
          case (from: DecimalType, to: DecimalType, Some(divisor))
              if divisor.signum > 0 && divisor.stripTrailingZeros.scale <= 0 &&
                to.scale >= from.scale && to.scale - from.scale <= 12 =>
            val shift = java.math.BigInteger.TEN.pow(to.scale - from.scale)
            val qr = shift.divideAndRemainder(divisor.toBigInteger)
            if (qr(1).signum == 0) Some((dividend, qr(0).longValueExact())) else None
          case _ => None
        }
      case _ => None
    }
  }

  /** Variance and standard deviation, sample or population: the second central moment. */
  private[gpu] def secondMoment(m: CentralMomentAgg): Boolean = m match {
    case _: StddevSamp | _: StddevPop | _: VarianceSamp | _: VariancePop => true
    case _ => false
  }

  /** An int or bigint column, possibly cast to double. */
  private[gpu] def integralColumn(e: Expression): Option[AttributeReference] = e match {
    case Cast(a: AttributeReference, DoubleType, _, _)
        if a.dataType == IntegerType || a.dataType == LongType =>
      Some(a)
    case a: AttributeReference if a.dataType == IntegerType || a.dataType == LongType => Some(a)
    case _ => None
  }

  /** {@code e} as signed columns, each accepted by {@code column}. */
  private def signedColumns(
      e: Expression,
      column: Expression => Boolean): Option[Seq[(Double, AttributeReference)]] = e match {
    case Add(l, r, _) => for (a <- signedColumns(l, column); b <- signedColumns(r, column)) yield a ++ b
    case Subtract(l, r, _) =>
      for (a <- signedColumns(l, column); b <- signedColumns(r, column))
        yield a ++ b.map { case (s, c) => (-s, c) }
    case Cast(c, to: DecimalType, _, _) if (c.dataType match {
          case from: DecimalType => from.scale == to.scale && from.precision <= to.precision
          case _ => false
        }) =>
      signedColumns(c, column)
    case Cast(a: AttributeReference, LongType, _, _) if a.dataType == IntegerType && column(a) =>
      Some(Seq(1.0 -> a))
    case a: AttributeReference if column(a) => Some(Seq(1.0 -> a))
    case _ => None
  }

  private def exactTerms(
      table: org.apache.iceberg.Table,
      e: Expression,
      nulls: Boolean,
      column: Expression => Boolean): Either[String, DeviceValue] =
    signedColumns(e, column) match {
      case Some(terms) if terms.size <= DeviceValue.MAX_FACTORS =>
        for {
          names <- traverse(terms.map(_._2))(a => field(table, a).map(_.name()))
          _ <- traverse(terms.map(_._2))(a => valueMayBeNull(table, a, nulls))
          _ <-
            if (GpuAggregates.exactTerms(table, names.map(n => List(n).asJava).asJava)) Right(())
            else Left(s"sum of $e may not be exact in doubles")
        } yield terms.zip(names).tail.foldLeft(
          if (terms.head._1 > 0) DeviceValue.of(names.head) else DeviceValue.of(names.head).negate()) {
          case (value, ((sign, _), name)) =>
            if (sign > 0) value.plus(DeviceValue.of(name)) else value.minus(DeviceValue.of(name))
        }
      case _ => Left(s"aggregate of $e")
    }

  private def exactType(dataType: DataType): Boolean = dataType match {
    case IntegerType | LongType => true
    case d: DecimalType => d.precision <= 18
    case _ => false
  }

  private sealed trait Factor
  private case class Plain(column: String) extends Factor
  private case class OneMinus(column: String) extends Factor
  private case class OnePlus(column: String) extends Factor

  /**
   * A product of numeric columns and `1 - c`, `1 + c` factors, with at least one plain column: what
   * [[DeviceValue]] evaluates. A row where any factor's column is null adds nothing to the sum and
   * is not counted in the value's non-null count, AVG's divisor (SQL's semantics for the product).
   */
  private def deviceValue(
      table: org.apache.iceberg.Table,
      expr: Expression,
      nulls: Boolean): Either[String, DeviceValue] = {
    def column(e: Expression): Either[String, String] = e match {
      case a: AttributeReference if a.dataType == DoubleType => numeric(a)
      case Cast(a: AttributeReference, DoubleType, _, _)
          if a.dataType == IntegerType || a.dataType == LongType =>
        numeric(a)
      case other => Left(s"not a numeric column: $other")
    }
    def numeric(a: AttributeReference): Either[String, String] =
      field(table, a).flatMap { f =>
        f.`type`().typeId() match {
          case TypeID.DOUBLE | TypeID.INTEGER | TypeID.LONG =>
            valueMayBeNull(table, a, nulls).map(_ => f.name())
          case other => Left(s"not a device value column: $other")
        }
      }
    def one(l: Literal): Boolean = l.value != null && l.value.toString.toDouble == 1.0
    def factors(e: Expression): Either[String, Seq[Factor]] = e match {
      case Multiply(left, right, _) =>
        for (l <- factors(left); r <- factors(right)) yield l ++ r
      case Subtract(l: Literal, c, _) if one(l) => column(c).map(name => Seq(OneMinus(name)))
      case Add(l: Literal, c, _) if one(l) => column(c).map(name => Seq(OnePlus(name)))
      case Add(c, l: Literal, _) if one(l) => column(c).map(name => Seq(OnePlus(name)))
      case c => column(c).map(name => Seq(Plain(name)))
    }
    factors(expr).flatMap { all =>
      val (plain, rest) = all.partition(_.isInstanceOf[Plain])
      if (plain.isEmpty) {
        Left(s"no plain column factor: $expr")
      } else if (all.size > DeviceValue.MAX_FACTORS) {
        Left(s"more than ${DeviceValue.MAX_FACTORS} factors: $expr")
      } else {
        val first = DeviceValue.of(plain.head.asInstanceOf[Plain].column)
        Right((plain.tail ++ rest).foldLeft(first) {
          case (value, Plain(c)) => value.times(c)
          case (value, OneMinus(c)) => value.timesOneMinus(c)
          case (value, OnePlus(c)) => value.timesOnePlus(c)
        })
      }
    }
  }

  /** A value column: null-free, or nullable values allowed. */
  private def valueMayBeNull(
      table: org.apache.iceberg.Table,
      a: Attribute,
      nulls: Boolean): Either[String, Unit] =
    if (nulls) Right(()) else nullFree(table, a)

  private def nullFree(table: org.apache.iceberg.Table, a: Attribute): Either[String, Unit] =
    if (!a.nullable || GpuAggregates.nullFree(table, a.name)) Right(())
    else Left(s"${a.name} may hold nulls")

  private[gpu] def traverse[A, B](items: Seq[A])(f: A => Either[String, B]): Either[String, Seq[B]] =
    items.foldLeft(Right(Vector.empty): Either[String, Vector[B]]) { (acc, item) =>
      for (done <- acc; next <- f(item)) yield done :+ next
    }
}
