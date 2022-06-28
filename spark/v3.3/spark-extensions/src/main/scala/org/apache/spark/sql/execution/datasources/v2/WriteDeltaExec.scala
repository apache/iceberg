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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.SparkEnv
import org.apache.spark.SparkException
import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.util.RowDeltaUtils._
import org.apache.spark.sql.catalyst.util.WriteDeltaProjections
import org.apache.spark.sql.connector.iceberg.write.DeltaWrite
import org.apache.spark.sql.connector.iceberg.write.DeltaWriter
import org.apache.spark.sql.connector.write.BatchWrite
import org.apache.spark.sql.connector.write.DataWriter
import org.apache.spark.sql.connector.write.DataWriterFactory
import org.apache.spark.sql.connector.write.PhysicalWriteInfoImpl
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.CustomMetrics
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.util.LongAccumulator
import org.apache.spark.util.Utils
import scala.collection.compat.immutable.ArraySeq
import scala.util.control.NonFatal

/**
 * Physical plan node to write a delta of rows to an existing table.
 */
case class WriteDeltaExec(
    query: SparkPlan,
    refreshCache: () => Unit,
    projections: WriteDeltaProjections,
    write: DeltaWrite) extends ExtendedV2ExistingTableWriteExec[DeltaWriter[InternalRow]] {

  override lazy val references: AttributeSet = query.outputSet
  override lazy val stringArgs: Iterator[Any] = Iterator(query, write)

  override lazy val writingTask: WritingSparkTask[DeltaWriter[InternalRow]] = {
    DeltaWithMetadataWritingSparkTask(projections)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): WriteDeltaExec = {
    copy(query = newChild)
  }
}

// a trait similar to V2ExistingTableWriteExec but supports custom write tasks
trait ExtendedV2ExistingTableWriteExec[W <: DataWriter[InternalRow]] extends V2ExistingTableWriteExec {
  def writingTask: WritingSparkTask[W]

  protected override def writeWithV2(batchWrite: BatchWrite): Seq[InternalRow] = {
    val rdd: RDD[InternalRow] = {
      val tempRdd = query.execute()
      // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
      // partition rdd to make sure we at least set up one write task to write the metadata.
      if (tempRdd.partitions.length == 0) {
        sparkContext.parallelize(Seq.empty[InternalRow], 1)
      } else {
        tempRdd
      }
    }
    // introduce a local var to avoid serializing the whole class
    val task = writingTask
    val writerFactory = batchWrite.createBatchWriterFactory(
      PhysicalWriteInfoImpl(rdd.getNumPartitions))
    val useCommitCoordinator = batchWrite.useCommitCoordinator
    val messages = new Array[WriterCommitMessage](rdd.partitions.length)
    val totalNumRowsAccumulator = new LongAccumulator()

    logInfo(s"Start processing data source write support: $batchWrite. " +
      s"The input RDD has ${messages.length} partitions.")

    // Avoid object not serializable issue.
    val writeMetrics: Map[String, SQLMetric] = customMetrics

    try {
      sparkContext.runJob(
        rdd,
        (context: TaskContext, iter: Iterator[InternalRow]) =>
          task.run(writerFactory, context, iter, useCommitCoordinator, writeMetrics),
        rdd.partitions.indices,
        (index, result: DataWritingSparkTaskResult) => {
          val commitMessage = result.writerCommitMessage
          messages(index) = commitMessage
          totalNumRowsAccumulator.add(result.numRows)
          batchWrite.onDataWriterCommit(commitMessage)
        }
      )

      logInfo(s"Data source write support $batchWrite is committing.")
      batchWrite.commit(messages)
      logInfo(s"Data source write support $batchWrite committed.")
      commitProgress = Some(StreamWriterCommitProgress(totalNumRowsAccumulator.value))
    } catch {
      case cause: Throwable =>
        logError(s"Data source write support $batchWrite is aborting.")
        try {
          batchWrite.abort(messages)
        } catch {
          case t: Throwable =>
            logError(s"Data source write support $batchWrite failed to abort.")
            cause.addSuppressed(t)
            throw QueryExecutionErrors.writingJobFailedError(cause)
        }
        logError(s"Data source write support $batchWrite aborted.")
        cause match {
          // Only wrap non fatal exceptions.
          case NonFatal(e) => throw QueryExecutionErrors.writingJobAbortedError(e)
          case _ => throw cause
        }
    }

    Nil
  }
}

trait WritingSparkTask[W <: DataWriter[InternalRow]] extends Logging with Serializable {

  protected def writeFunc(writer: W, row: InternalRow): Unit

  def run(
      writerFactory: DataWriterFactory,
      context: TaskContext,
      iter: Iterator[InternalRow],
      useCommitCoordinator: Boolean,
      customMetrics: Map[String, SQLMetric]): DataWritingSparkTaskResult = {
    val stageId = context.stageId()
    val stageAttempt = context.stageAttemptNumber()
    val partId = context.partitionId()
    val taskId = context.taskAttemptId()
    val attemptId = context.attemptNumber()
    val dataWriter = writerFactory.createWriter(partId, taskId).asInstanceOf[W]

    var count = 0L
    // write the data and commit this writer.
    Utils.tryWithSafeFinallyAndFailureCallbacks(block = {
      while (iter.hasNext) {
        if (count % CustomMetrics.NUM_ROWS_PER_UPDATE == 0) {
          CustomMetrics.updateMetrics(ArraySeq.unsafeWrapArray(dataWriter.currentMetricsValues), customMetrics)
        }

        // Count is here.
        count += 1
        writeFunc(dataWriter, iter.next())
      }

      CustomMetrics.updateMetrics(ArraySeq.unsafeWrapArray(dataWriter.currentMetricsValues), customMetrics)

      val msg = if (useCommitCoordinator) {
        val coordinator = SparkEnv.get.outputCommitCoordinator
        val commitAuthorized = coordinator.canCommit(stageId, stageAttempt, partId, attemptId)
        if (commitAuthorized) {
          logInfo(s"Commit authorized for partition $partId (task $taskId, attempt $attemptId, " +
            s"stage $stageId.$stageAttempt)")
          dataWriter.commit()
        } else {
          val commitDeniedException = QueryExecutionErrors.commitDeniedError(
            partId, taskId, attemptId, stageId, stageAttempt)
          logInfo(commitDeniedException.getMessage)
          // throwing CommitDeniedException will trigger the catch block for abort
          throw commitDeniedException
        }

      } else {
        logInfo(s"Writer for partition ${context.partitionId()} is committing.")
        dataWriter.commit()
      }

      logInfo(s"Committed partition $partId (task $taskId, attempt $attemptId, " +
        s"stage $stageId.$stageAttempt)")

      DataWritingSparkTaskResult(count, msg)

    })(catchBlock = {
      // If there is an error, abort this writer
      logError(s"Aborting commit for partition $partId (task $taskId, attempt $attemptId, " +
        s"stage $stageId.$stageAttempt)")
      dataWriter.abort()
      logError(s"Aborted commit for partition $partId (task $taskId, attempt $attemptId, " +
        s"stage $stageId.$stageAttempt)")
    }, finallyBlock = {
      dataWriter.close()
    })
  }
}

case class DeltaWithMetadataWritingSparkTask(
    projs: WriteDeltaProjections) extends WritingSparkTask[DeltaWriter[InternalRow]] {

  private lazy val rowProjection = projs.rowProjection.orNull
  private lazy val rowIdProjection = projs.rowIdProjection
  private lazy val metadataProjection = projs.metadataProjection.orNull

  override protected def writeFunc(writer: DeltaWriter[InternalRow], row: InternalRow): Unit = {
    val operation = row.getInt(0)

    operation match {
      case DELETE_OPERATION =>
        rowIdProjection.project(row)
        metadataProjection.project(row)
        writer.delete(metadataProjection, rowIdProjection)

      case UPDATE_OPERATION =>
        rowProjection.project(row)
        rowIdProjection.project(row)
        metadataProjection.project(row)
        writer.update(metadataProjection, rowIdProjection, rowProjection)

      case INSERT_OPERATION =>
        rowProjection.project(row)
        writer.insert(rowProjection)

      case other =>
        throw new SparkException(s"Unexpected operation ID: $other")
    }
  }
}
