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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Left => _, Right => _, _}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count, Sum}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._

/**
 * An aggregate over a UNION ALL (TPC-DS q23a's `sum(sales)` over catalog and web sales): each
 * branch is an aggregate of its own -- grouped by the union's group keys over the branch's
 * columns -- planned for the device like any other (a star, say), and the branches' partial
 * results are merged on the driver by group. SUM and COUNT only, whose partials add up: a SUM is
 * the sum of the branches' non-null sums (null when all are), a COUNT the sum of the counts.
 */
object UnionAggregates {
  val Enabled = "spark.iceberg.gpu.aggregate.union"

  import GpuAggregateStrategy.traverse

  def spec(
      agg: Aggregate,
      branchSpec: Aggregate => Either[String, DeviceAggregateSpec]): Either[String, GpuUnionAggregateSpec] =
    agg.child match {
      case union: Union if !union.byName && !union.allowMissingCol =>
        val aggregates = agg.aggregateExpressions.flatMap(_.collect { case ae: AggregateExpression => ae }).distinct
        val keys = agg.groupingExpressions
        for {
          _ <- if (keys.forall(_.deterministic)) Right(()) else Left("nondeterministic group keys")
          _ <- traverse(aggregates) { ae =>
            ae.aggregateFunction match {
              case _ if ae.isDistinct || ae.filter.isDefined => Left(s"DISTINCT or FILTER: $ae")
              case _: Sum | _: Count => Right(())
              case other => Left(s"over a union: $other")
            }
          }
          branches <- traverse(union.children) { child =>
            // The union's output attributes are its first child's; each branch reads its own.
            val byPosition = AttributeMap(union.output.zip(child.output))
            def own(e: Expression) = e.transform { case a: Attribute if byPosition.contains(a) => byPosition(a) }
            val outputs =
              keys.zipWithIndex.map { case (k, i) => Alias(own(k), s"key_$i")() } ++
                aggregates.zipWithIndex.map { case (ae, k) => Alias(own(ae), s"part_$k")() }
            val branch = Aggregate(keys.map(own), outputs, child)
            branchSpec(branch).left.map(reason => s"branch: $reason").map(branch -> _)
          }
        } yield GpuUnionAggregateSpec(branches, keys, aggregates, agg.aggregateExpressions)
      case _: Union => Left("a union by name")
      case other => Left(s"not a union: ${other.nodeName}")
    }
}

/**
 * An aggregate over a union as [[UnionAggregates]] plans it: each branch's rows (group keys, then
 * partials), merged by group.
 */
case class GpuUnionAggregateSpec(
    branches: Seq[(Aggregate, DeviceAggregateSpec)],
    keys: Seq[Expression],
    aggregates: Seq[AggregateExpression],
    resultExpressions: Seq[NamedExpression])
    extends DeviceAggregateSpec
    with org.apache.spark.internal.Logging {

  override def run(): Seq[InternalRow] = {
    val start = System.nanoTime()
    val g = keys.size
    val merged = scala.collection.mutable.LinkedHashMap[Seq[Any], Array[Any]]()
    branches.foreach { case (branch, spec) =>
      val rows =
        try spec.run()
        catch {
          case e: RuntimeException =>
            // The branch plans again elsewhere (a subquery's own aggregate): decline it then.
            spec match {
              case _: GpuStarAggregateSpec => StarAggregates.remember(branch)
              case _ =>
            }
            throw e
        }
      rows.foreach { row =>
        val key = keys.indices.map(i => row.get(i, keys(i).dataType))
        val totals = merged.getOrElseUpdate(key, Array.tabulate[Any](aggregates.size) { k =>
          aggregates(k).aggregateFunction match {
            case _: Count => 0L
            case _ => null
          }
        })
        aggregates.indices.foreach { k =>
          if (!row.isNullAt(g + k)) {
            totals(k) = add(aggregates(k).dataType, totals(k), row.get(g + k, aggregates(k).dataType))
          }
        }
      }
    }
    if (merged.isEmpty && keys.isEmpty) {
      // A global aggregate has its one row even when every branch is empty.
      merged(Nil) = Array.tabulate[Any](aggregates.size) { k =>
        if (aggregates(k).aggregateFunction.isInstanceOf[Count]) 0L else null
      }
    }
    // The result expressions over a row of the group keys, then the totals.
    val bound = resultExpressions.map { e =>
      e.transformDown {
        case ae: AggregateExpression =>
          val k = aggregates.indexWhere(_.semanticEquals(ae))
          BoundReference(g + k, ae.dataType, nullable = true)
        case k if keys.exists(_.semanticEquals(k)) =>
          val i = keys.indexWhere(_.semanticEquals(k))
          BoundReference(i, k.dataType, k.nullable)
      }
    }
    val result = merged.toSeq.map { case (key, totals) =>
      val input = InternalRow.fromSeq(key ++ totals.indices.map(k => finish(aggregates(k).dataType, totals(k))))
      InternalRow.fromSeq(bound.map(_.eval(input)))
    }
    logInfo(
      f"union aggregate: ${branches.size} branches (${branches.map(_._2.getClass.getSimpleName).mkString(", ")})," +
        f" ${merged.size} groups, ${(System.nanoTime() - start) / 1e6}%.1f ms")
    result
  }

  private def add(dataType: DataType, total: Any, value: Any): Any =
    if (total == null) value
    else
      dataType match {
        case LongType => total.asInstanceOf[Long] + value.asInstanceOf[Long]
        case DoubleType => total.asInstanceOf[Double] + value.asInstanceOf[Double]
        case _: DecimalType => total.asInstanceOf[Decimal] + value.asInstanceOf[Decimal]
        case other => throw new UnsupportedOperationException(s"SUM of $other over a union")
      }

  // Spark's SUM of a decimal is null when the result overflows its type.
  private def finish(dataType: DataType, total: Any): Any = (dataType, total) match {
    case (d: DecimalType, sum: Decimal) => if (sum.changePrecision(d.precision, d.scale)) sum else null
    case _ => total
  }
}
