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
package org.apache.iceberg;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DVUtil {
  private static final Logger LOG = LoggerFactory.getLogger(DVUtil.class);

  private DVUtil() {}

  /**
   * Merges duplicate DVs for the same data file and writes the merged DV Puffin files.
   *
   * @param duplicateDVsByReferencedFile map of data file location to duplicate DVs (all entries
   *     must have size > 1)
   * @return newly merged DVs
   */
  static List<DeleteFile> mergeDVsAndWrite(
      TableOperations ops,
      Map<String, List<DeleteFile>> duplicateDVsByReferencedFile,
      String tableName,
      ExecutorService threadpool) {
    Map<String, PositionDeleteIndex> mergedIndices =
        duplicateDVsByReferencedFile.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> readDVsAndMerge(ops, entry.getValue(), threadpool)));

    return writeMergedDVs(
        mergedIndices, duplicateDVsByReferencedFile, ops, tableName, ops.current().specsById());
  }

  // Merges the position indices for the duplicate DVs for a given referenced file
  private static PositionDeleteIndex readDVsAndMerge(
      TableOperations ops, List<DeleteFile> dvsForFile, ExecutorService pool) {
    Preconditions.checkArgument(dvsForFile.size() > 1, "Expected more than 1 DV");
    PositionDeleteIndex[] dvIndices = readDVs(dvsForFile, pool, ops);
    PositionDeleteIndex mergedPositions = dvIndices[0];
    DeleteFile firstDV = dvsForFile.get(0);

    for (int i = 1; i < dvIndices.length; i++) {
      DeleteFile dv = dvsForFile.get(i);
      Preconditions.checkArgument(
          Objects.equals(dv.dataSequenceNumber(), firstDV.dataSequenceNumber()),
          "Cannot merge duplicate added DVs when data sequence numbers are different, "
              + "expected all to be added with sequence %s, but got %s",
          firstDV.dataSequenceNumber(),
          dv.dataSequenceNumber());

      Preconditions.checkArgument(
          dv.specId() == firstDV.specId(),
          "Cannot merge duplicate added DVs when partition specs are different, "
              + "expected all to be added with spec %s, but got %s",
          firstDV.specId(),
          dv.specId());

      Preconditions.checkArgument(
          Objects.equals(dv.partition(), firstDV.partition()),
          "Cannot merge duplicate added DVs when partition tuples are different");

      mergedPositions.merge(dvIndices[i]);
    }

    return mergedPositions;
  }

  private static PositionDeleteIndex[] readDVs(
      List<DeleteFile> dvs, ExecutorService pool, TableOperations ops) {
    PositionDeleteIndex[] dvIndices = new PositionDeleteIndex[dvs.size()];
    Tasks.range(dvIndices.length)
        .executeWith(pool)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .run(
            i -> {
              dvIndices[i] = Deletes.readDV(dvs.get(i), ops.io(), ops.encryption());
            });

    return dvIndices;
  }

  // Produces a Puffin per partition spec containing the merged DVs for that spec
  private static List<DeleteFile> writeMergedDVs(
      Map<String, PositionDeleteIndex> mergedIndices,
      Map<String, List<DeleteFile>> dataFilesWithDuplicateDVs,
      TableOperations ops,
      String tableName,
      Map<Integer, PartitionSpec> specsById) {
    try (DVFileWriter dvFileWriter =
        new BaseDVFileWriter(
            // Use an unpartitioned spec for the location provider for the puffin containing
            // all the merged DVs
            OutputFileFactory.builderFor(
                    ops, PartitionSpec.unpartitioned(), FileFormat.PUFFIN, 1, 1)
                .build(),
            path -> null)) {

      for (Map.Entry<String, PositionDeleteIndex> entry : mergedIndices.entrySet()) {
        String referencedLocation = entry.getKey();
        PositionDeleteIndex mergedPositions = entry.getValue();
        List<DeleteFile> duplicateDVs = dataFilesWithDuplicateDVs.get(referencedLocation);
        DeleteFile firstDV = duplicateDVs.get(0);
        LOG.warn(
            "Merged {} DVs for data file {}. These will be orphaned DVs in table {}",
            duplicateDVs.size(),
            referencedLocation,
            tableName);
        dvFileWriter.delete(
            referencedLocation,
            mergedPositions,
            specsById.get(firstDV.specId()),
            firstDV.partition());
      }

      dvFileWriter.close();
      DeleteWriteResult writeResult = dvFileWriter.result();
      return writeResult.deleteFiles();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
