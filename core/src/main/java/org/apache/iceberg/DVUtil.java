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
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.Deletes;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
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
  static List<DeleteFile> mergeAndWriteDVsIfRequired(
      Map<String, List<DeleteFile>> dvsByFile,
      Supplier<OutputFile> dvOutputFile,
      FileIO fileIO,
      Map<Integer, PartitionSpec> specs,
      ExecutorService pool) {
    Map<String, List<DeleteFile>> duplicateDVsByFile =
        dvsByFile.entrySet().stream()
            .filter(e -> e.getValue().size() > 1)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    if (duplicateDVsByFile.isEmpty()) {
      return dvsByFile.values().stream().flatMap(List::stream).collect(Collectors.toList());
    }

    validateCanMerge(duplicateDVsByFile, specs);
    List<DeleteFile> duplicateDVs =
        duplicateDVsByFile.values().stream().flatMap(List::stream).collect(Collectors.toList());
    Map<String, PositionDeleteIndex> mergedPositionsForFile =
        readAndMergeDVs(duplicateDVs, fileIO, pool);

    List<DeleteFile> mergedDVs = Lists.newArrayList();
    Map<String, PartitionSpec> specByFile = Maps.newHashMap();
    Map<String, StructLike> partitionByFile = Maps.newHashMap();
    for (List<DeleteFile> dvs : dvsByFile.values()) {
      DeleteFile firstDV = dvs.get(0);
      if (dvs.size() == 1) {
        mergedDVs.add(firstDV);
      } else {
        specByFile.put(firstDV.referencedDataFile(), specs.get(firstDV.specId()));
        partitionByFile.put(firstDV.referencedDataFile(), firstDV.partition());
      }
    }

    mergedDVs.addAll(writeDVs(mergedPositionsForFile, specByFile, partitionByFile, dvOutputFile));
    return mergedDVs;
  }

  private static void validateCanMerge(
      Map<String, List<DeleteFile>> duplicateDVsByFile, Map<Integer, PartitionSpec> specs) {
    Map<Integer, Comparator<StructLike>> comparatorsBySpecId = Maps.newHashMap();
    for (List<DeleteFile> dvs : duplicateDVsByFile.values()) {
      DeleteFile firstDV = dvs.get(0);
      Comparator<StructLike> comparator =
          comparatorsBySpecId.computeIfAbsent(
              firstDV.specId(), id -> Comparators.forType(specs.get(id).partitionType()));
      for (int i = 1; i < dvs.size(); i++) {
        validateCanMerge(firstDV, dvs.get(i), comparator);
      }
    }
  }

  private static void validateCanMerge(
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

  /**
   * Reads all DVs, and merge the position indices per referenced data file
   *
   * @param duplicateDVs list of dvs to read and merge
   * @param io the FileIO to use for reading DV files
   * @param pool executor service for reading DVs
   * @return map of referenced data file location to the merged position delete index
   */
  private static Map<String, PositionDeleteIndex> readAndMergeDVs(
      List<DeleteFile> duplicateDVs, FileIO io, ExecutorService pool) {
    // Read all duplicate DVs in parallel
    PositionDeleteIndex[] duplicateDVPositions = new PositionDeleteIndex[duplicateDVs.size()];
    Tasks.range(duplicateDVPositions.length)
        .executeWith(pool)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .run(i -> duplicateDVPositions[i] = Deletes.readDV(duplicateDVs.get(i), io));

    Map<String, PositionDeleteIndex> mergedIndexByFile = Maps.newHashMap();
    for (int i = 0; i < duplicateDVPositions.length; i++) {
      PositionDeleteIndex dvIndices = duplicateDVPositions[i];
      DeleteFile dv = duplicateDVs.get(i);
      mergedIndexByFile.merge(
          dv.referencedDataFile(),
          dvIndices,
          (mergedIndex, newIndex) -> {
            mergedIndex.merge(newIndex);
            return mergedIndex;
          });
    }

    return mergedIndexByFile;
  }

  // Produces a single Puffin file containing the merged DVs
  private static List<DeleteFile> writeDVs(
      Map<String, PositionDeleteIndex> mergedIndexByFile,
      Map<String, PartitionSpec> specByFile,
      Map<String, StructLike> partitionByFile,
      Supplier<OutputFile> dvOutputFile) {
    try (DVFileWriter dvFileWriter = new BaseDVFileWriter(dvOutputFile, path -> null)) {
      for (Map.Entry<String, PositionDeleteIndex> entry : mergedIndexByFile.entrySet()) {
        String referencedLocation = entry.getKey();
        PositionDeleteIndex mergedPositions = entry.getValue();
        dvFileWriter.delete(
            referencedLocation,
            mergedPositions,
            specByFile.get(referencedLocation),
            partitionByFile.get(referencedLocation));
      }

      dvFileWriter.close();
      return dvFileWriter.result().deleteFiles();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
