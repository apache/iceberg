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
import java.util.stream.Collectors;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.encryption.EncryptionManager;
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
   * @param dvsByFile map of data file location to DVs
   * @return a list containing both any newly merged DVs and any DVs that are already valid
   */
  static List<DeleteFile> mergeAndWriteDvsIfRequired(
      Map<String, List<DeleteFile>> dvsByFile,
      ExecutorService pool,
      LocationProvider locationProvider,
      EncryptionManager encryptionManager,
      FileIO fileIO,
      Map<Integer, PartitionSpec> specs) {
    List<DeleteFile> finalDvs = Lists.newArrayList();
    Map<String, List<DeleteFile>> duplicateDvsByFile = Maps.newLinkedHashMap();
    for (Map.Entry<String, List<DeleteFile>> dvsForFile : dvsByFile.entrySet()) {
      List<DeleteFile> dvs = dvsForFile.getValue();
      if (!dvs.isEmpty()) {
        if (dvs.size() == 1) {
          finalDvs.addAll(dvs);
        } else {
          duplicateDvsByFile.put(dvsForFile.getKey(), dvs);
        }
      }
    }

    if (!duplicateDvsByFile.isEmpty()) {
      List<DeleteFile> duplicateDvs =
          duplicateDvsByFile.values().stream().flatMap(List::stream).collect(Collectors.toList());

      Map<String, PositionDeleteIndex> mergedIndices =
          readAndMergeDvs(duplicateDvs, pool, fileIO, encryptionManager, specs);
      finalDvs.addAll(
          writeMergedDVs(
              mergedIndices,
              duplicateDvsByFile,
              locationProvider,
              encryptionManager,
              fileIO,
              specs));
    }

    return finalDvs;
  }

  /**
   * Reads all DVs, and merge the position indices per referenced data file
   *
   * @param io the FileIO to use for reading DV files
   * @param encryptionManager the EncryptionManager for decrypting DV files
   * @param duplicateDvs list of dvs to read and merge
   * @param specsById map of partition spec ID to partition spec
   * @param pool executor service for reading DVs
   * @return map of referenced data file location to the merged position delete index
   */
  private static Map<String, PositionDeleteIndex> readAndMergeDvs(
      List<DeleteFile> duplicateDvs,
      ExecutorService pool,
      FileIO io,
      EncryptionManager encryptionManager,
      Map<Integer, PartitionSpec> specsById) {
    // Read all duplicate DVs in parallel
    PositionDeleteIndex[] duplicateDvPositions = new PositionDeleteIndex[duplicateDvs.size()];
    Tasks.range(duplicateDvPositions.length)
        .executeWith(pool)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .run(
            i ->
                duplicateDvPositions[i] =
                    Deletes.readDV(duplicateDvs.get(i), io, encryptionManager));

    // Build a grouping of referenced file to indices of the corresponding duplicate DVs
    Map<String, List<Integer>> dvIndicesByDataFile = Maps.newLinkedHashMap();
    for (int i = 0; i < duplicateDvs.size(); i++) {
      dvIndicesByDataFile
          .computeIfAbsent(duplicateDvs.get(i).referencedDataFile(), k -> Lists.newArrayList())
          .add(i);
    }

    // Validate and merge per referenced file, caching comparators by spec ID
    Map<Integer, Comparator<StructLike>> comparatorsBySpecId = Maps.newHashMap();
    Map<String, PositionDeleteIndex> result = Maps.newHashMap();
    for (Map.Entry<String, List<Integer>> entry : dvIndicesByDataFile.entrySet()) {
      List<Integer> dvIndicesForFile = entry.getValue();
      int firstDVIndex = dvIndicesForFile.get(0);
      PositionDeleteIndex mergedIndexForFile = duplicateDvPositions[firstDVIndex];
      DeleteFile firstDv = duplicateDvs.get(firstDVIndex);

      Comparator<StructLike> partitionComparator =
          comparatorsBySpecId.computeIfAbsent(
              firstDv.specId(), id -> Comparators.forType(specsById.get(id).partitionType()));

      for (int i = 1; i < dvIndicesForFile.size(); i++) {
        int dvIndex = dvIndicesForFile.get(i);
        DeleteFile dv = duplicateDvs.get(dvIndex);
        validateDVCanBeMerged(dv, firstDv, partitionComparator);
        mergedIndexForFile.merge(duplicateDvPositions[dvIndex]);
      }

      result.put(entry.getKey(), mergedIndexForFile);
    }

    return result;
  }

  private static void validateDVCanBeMerged(
      DeleteFile first, DeleteFile second, Comparator<StructLike> partitionComparator) {
    Preconditions.checkArgument(
        Objects.equals(first.dataSequenceNumber(), second.dataSequenceNumber()),
        "Cannot merge duplicate added DVs when data sequence numbers are different, "
            + "expected all to be added with sequence %s, but got %s",
        first.dataSequenceNumber(),
        second.dataSequenceNumber());

    Preconditions.checkArgument(
        first.specId() == second.specId(),
        "Cannot merge duplicate added DVs when partition specs are different, "
            + "expected all to be added with spec %s, but got %s",
        first.specId(),
        second.specId());

    Preconditions.checkArgument(
        partitionComparator.compare(first.partition(), second.partition()) == 0,
        "Cannot merge duplicate added DVs when partition tuples are different");
  }

  // Produces a single Puffin file containing the merged DVs
  private static List<DeleteFile> writeMergedDVs(
      Map<String, PositionDeleteIndex> mergedIndices,
      Map<String, List<DeleteFile>> dvsByFile,
      LocationProvider locationProvider,
      EncryptionManager encryptionManager,
      FileIO fileIO,
      Map<Integer, PartitionSpec> specsById) {
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
        List<DeleteFile> duplicateDvs = dvsByFile.get(referencedLocation);
        DeleteFile firstDV = duplicateDvs.get(0);
        dvFileWriter.delete(
            referencedLocation,
            mergedPositions,
            specsById.get(firstDV.specId()),
            firstDV.partition());
      }

      dvFileWriter.close();
      return dvFileWriter.result().deleteFiles();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
