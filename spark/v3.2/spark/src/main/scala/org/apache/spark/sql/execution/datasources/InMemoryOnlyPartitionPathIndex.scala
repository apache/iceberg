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

package org.apache.spark.sql.execution.datasources
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import scala.collection.mutable


/**
 * Implement our own in-memory index which will only list directories to avoid unnecessary file
 * listings. Should ONLY be used to get partition directory paths. Uses table's schema to only
 * visit partition dirs using number of partition columns depth recursively. Does NOT return files
 * within leaf dir.
 */
class InMemoryOnlyPartitionPathIndex(
                         sparkSession: SparkSession,
                         rootPath :Path,
                         parameters: Map[String, String],
                         userSpecifiedSchema: StructType,
                         fileStatusCache: FileStatusCache = NoopCache,
                         override val metadataOpsTimeNs: Option[Long] = None)
  extends PartitioningAwareFileIndex(
    sparkSession, parameters, Some(userSpecifiedSchema), fileStatusCache) {

  override val rootPaths = Seq(rootPath)

  @volatile private var cachedLeafFiles: mutable.LinkedHashMap[Path, FileStatus] = _
  @volatile private var cachedLeafDirToChildrenFiles: Map[Path, Array[FileStatus]] = _
  @volatile private var cachedPartitionSpec: PartitionSpec = _

  refresh0()

  override def partitionSpec(): PartitionSpec = {
    if (cachedPartitionSpec == null) {
        cachedPartitionSpec = inferPartitioning()
    }
    logTrace(s"Partition spec: $cachedPartitionSpec")
    cachedPartitionSpec
  }

  override protected def leafFiles: mutable.LinkedHashMap[Path, FileStatus] = cachedLeafFiles

  override protected def leafDirToChildrenFiles: Map[Path, Array[FileStatus]] = cachedLeafDirToChildrenFiles

  override def refresh(): Unit = {
    fileStatusCache.invalidateAll()
    refresh0()
  }

  private def refresh0(): Unit = {
    val files = listLeafDirs(rootPath, 0)
    cachedLeafFiles =
      new mutable.LinkedHashMap[Path, FileStatus]() ++= files.map(f => f.getPath -> f)
    cachedLeafDirToChildrenFiles = files.map( f => (f.getPath, Array(createEmptyChildDataFileStatus(f))))
      .toMap
    cachedPartitionSpec = null
  }

  override def equals(other: Any): Boolean = other match {
    case hdfs: InMemoryFileIndex => rootPaths.toSet == hdfs.rootPaths.toSet
    case _ => false
  }

  override def hashCode(): Int = rootPaths.toSet.hashCode()

  /**
   * recursively lists only the partition dirs. Uses the number of partition cols
   * from user specified schema.
   * @param path
   * @param partitionIndex
   * @return
   */
  private def listLeafDirs(path: Path, partitionIndex: Int): mutable.LinkedHashSet[FileStatus] = {
    val startTime = System.nanoTime()
    val output = mutable.LinkedHashSet[FileStatus]()
    val numPartitions = userSpecifiedSchema.fields.length
    if (partitionIndex < numPartitions) {
      path.getFileSystem(sparkSession.sessionState.newHadoopConf())
        .listStatus(path)
        .filter(f => f.isDirectory)
        .foreach(f => {
          if (partitionIndex == numPartitions -1) {
            output.add(f)
          } else {
            output ++= listLeafDirs(f.getPath, partitionIndex + 1)
          }
        })
    }
    logInfo(s"It took ${(System.nanoTime() - startTime) / (1000 * 1000)} ms to list leaf dirs" +
      s" for path ${path}.")
    output
  }

  private def createEmptyChildDataFileStatus(fs: FileStatus) =
    new FileStatus(1L,
      false,
      fs.getReplication,
      1L,
      fs.getModificationTime,
      fs.getAccessTime,
      fs.getPermission,
      fs.getOwner,
      fs.getGroup,
      new Path(fs.getPath, fs.getPath.toString + "/dummyDataFile"))
}
