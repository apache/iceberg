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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.util.Tasks;

class DVUtil {
  private DVUtil() {}

  /**
   * Merges duplicate DVs for the same data file and writes the merged DV Puffin files. If there is
   * exactly 1 DV for a given data file then it is return as is
   *
   * @param dvsByReferencedFile map of data file location to DVs
   * @return a list containing both any newly merged DVs and any DVs that are already valid
   */
  static List<DeleteFile> mergeAndWriteDVsIfRequired(
      Map<String, List<DeleteFile>> dvsByReferencedFile,
      ExecutorService threadpool,
      LocationProvider locationProvider,
      EncryptionManager encryptionManager,
      FileIO fileIO,
      Map<Integer, PartitionSpec> specs) {
    List<DeleteFile> finalDVs = Lists.newArrayList();
    Map<String, List<DeleteFile>> duplicatesByRef = Maps.newLinkedHashMap();
    for (Map.Entry<String, List<DeleteFile>> dvsForFile : dvsByReferencedFile.entrySet()) {
      List<DeleteFile> dvs = dvsForFile.getValue();
      Preconditions.checkArgument(!dvs.isEmpty(), "Expected DV for file %s", dvsForFile.getKey());
      if (dvs.size() == 1) {
        finalDVs.addAll(dvs);
      } else {
        duplicatesByRef.put(dvsForFile.getKey(), dvs);
      }
    }

    if (!duplicatesByRef.isEmpty()) {
      List<DeleteFile> duplicateDVs = Lists.newArrayList();
      for (List<DeleteFile> duplicates : duplicatesByRef.values()) {
        duplicateDVs.addAll(duplicates);
      }

      Map<String, PositionDeleteIndex> mergedIndices =
          readAndMergeDVs(fileIO, encryptionManager, duplicateDVs, specs, threadpool);
      finalDVs.addAll(
          writeMergedDVs(
              mergedIndices, duplicatesByRef, locationProvider, encryptionManager, fileIO, specs));
    }

    return finalDVs;
  }

  /**
   * Reads all DVs in parallel in a single batch, then validates and merges the position indices per
   * referenced data file.
   *
   * @param io the FileIO to use for reading DV files
   * @param encryptionManager the EncryptionManager for decrypting DV files
   * @param duplicateDVs list of dvs to read and merge
   * @param specsById map of partition spec ID to partition spec
   * @param pool executor service for parallel DV reads
   * @return map of referenced data file location to merged position delete index
   */
  private static Map<String, PositionDeleteIndex> readAndMergeDVs(
      FileIO io,
      EncryptionManager encryptionManager,
      List<DeleteFile> duplicateDVs,
      Map<Integer, PartitionSpec> specsById,
      ExecutorService pool) {
    // Read all DVs into memory
    PositionDeleteIndex[] allDVPositions = new PositionDeleteIndex[duplicateDVs.size()];
    Tasks.range(allDVPositions.length)
        .executeWith(pool)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .run(i -> allDVPositions[i] = Deletes.readDV(duplicateDVs.get(i), io, encryptionManager));

    // Group DV indices by referenced data file
    Map<String, List<Integer>> dvIndicesByReferencedFile = Maps.newLinkedHashMap();
    for (int i = 0; i < duplicateDVs.size(); i++) {
      dvIndicesByReferencedFile
          .computeIfAbsent(duplicateDVs.get(i).referencedDataFile(), k -> Lists.newArrayList())
          .add(i);
    }

    // Validate and merge per referenced file, caching comparators by spec ID
    Map<Integer, Comparator<StructLike>> comparatorsBySpecId = Maps.newHashMap();
    Map<String, PositionDeleteIndex> result = Maps.newHashMap();
    for (Map.Entry<String, List<Integer>> entry : dvIndicesByReferencedFile.entrySet()) {
      List<Integer> dvIndicesForFile = entry.getValue();
      int firstDVIndex = dvIndicesForFile.get(0);
      PositionDeleteIndex merged = allDVPositions[firstDVIndex];
      DeleteFile firstDV = duplicateDVs.get(firstDVIndex);
      Comparator<StructLike> partitionComparator =
          comparatorsBySpecId.computeIfAbsent(
              firstDV.specId(), id -> Comparators.forType(specsById.get(id).partitionType()));

      for (int i = 1; i < dvIndicesForFile.size(); i++) {
        int dvIndex = dvIndicesForFile.get(i);
        DeleteFile dv = duplicateDVs.get(dvIndex);
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
            partitionComparator.compare(dv.partition(), firstDV.partition()) == 0,
            "Cannot merge duplicate added DVs when partition tuples are different");

        merged.merge(allDVPositions[dvIndex]);
      }

      result.put(entry.getKey(), merged);
    }

    return result;
  }

  // Produces a single Puffin file containing the merged DVs
  private static List<DeleteFile> writeMergedDVs(
      Map<String, PositionDeleteIndex> mergedIndices,
      Map<String, List<DeleteFile>> dvsByReferencedFile,
      LocationProvider locationProvider,
      EncryptionManager encryptionManager,
      FileIO fileIO,
      Map<Integer, PartitionSpec> specsById) {
    if (mergedIndices.isEmpty()) {
      return Lists.newArrayList();
    }

    try (DVFileWriter dvFileWriter =
        new BaseDVFileWriter(
            // Use an unpartitioned spec for the location provider for the puffin containing
            // all the merged DVs
            OutputFileFactory.builderFor(
                    locationProvider,
                    encryptionManager,
                    () -> fileIO,
                    PartitionSpec.unpartitioned(),
                    FileFormat.PUFFIN,
                    1,
                    1)
                .build(),
            path -> null)) {

      for (Map.Entry<String, PositionDeleteIndex> entry : mergedIndices.entrySet()) {
        String referencedLocation = entry.getKey();
        PositionDeleteIndex mergedPositions = entry.getValue();
        List<DeleteFile> duplicateDVs = dvsByReferencedFile.get(referencedLocation);
        DeleteFile firstDV = duplicateDVs.get(0);
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
