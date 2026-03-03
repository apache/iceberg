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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Multimap;
import org.apache.iceberg.relocated.com.google.common.collect.Multimaps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;

class DVUtil {
  private DVUtil() {}

  static PositionDeleteIndex readDV(DeleteFile deleteFile, FileIO fileIO) {
    Preconditions.checkArgument(
        ContentFileUtil.isDV(deleteFile),
        "Cannot read, not a deletion vector: %s",
        deleteFile.location());
    InputFile inputFile = fileIO.newInputFile(deleteFile);
    long offset = deleteFile.contentOffset();
    int length = deleteFile.contentSizeInBytes().intValue();
    byte[] bytes = new byte[length];
    try {
      IOUtil.readFully(inputFile, offset, bytes, 0, length);
      return PositionDeleteIndex.deserialize(bytes, deleteFile);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Merges duplicate DVs for the same data file and writes the merged DV Puffin files. If there is
   * exactly 1 DV for a given data file then it is return as is
   *
   * @param dvsByReferencedFile map of data file location to DVs
   * @param mergedOutputLocation output location of the merged DVs
   * @param fileIO fileIO to use when reading and writing
   * @param specs partition specs
   * @param pool executor service for reading DVs
   * @return a list containing both any newly merged DVs and any DVs that are already valid
   */
  static List<DeleteFile> mergeAndWriteDVsIfRequired(
      Map<String, List<DeleteFile>> dvsByReferencedFile,
      String mergedOutputLocation,
      FileIO fileIO,
      Map<Integer, PartitionSpec> specs,
      ExecutorService pool) {
    List<DeleteFile> finalDVs = Lists.newArrayList();
    Multimap<String, DeleteFile> duplicates =
        Multimaps.newListMultimap(Maps.newHashMap(), Lists::newArrayList);
    Map<String, Pair<PartitionSpec, StructLike>> partitions = Maps.newHashMap();

    for (Map.Entry<String, List<DeleteFile>> entry : dvsByReferencedFile.entrySet()) {
      if (entry.getValue().size() > 1) {
        duplicates.putAll(entry.getKey(), entry.getValue());
        DeleteFile first = entry.getValue().get(0);
        partitions.put(entry.getKey(), Pair.of(specs.get(first.specId()), first.partition()));
      } else {
        finalDVs.addAll(entry.getValue());
      }
    }

    if (duplicates.isEmpty()) {
      return finalDVs;
    }

    validateCanMerge(duplicates, partitions);

    Map<String, PositionDeleteIndex> deletes =
        readAndMergeDVs(duplicates.values().toArray(DeleteFile[]::new), fileIO, pool);

    finalDVs.addAll(writeDVs(deletes, fileIO, mergedOutputLocation, partitions));
    return finalDVs;
  }

  private static void validateCanMerge(
      Multimap<String, DeleteFile> duplicates,
      Map<String, Pair<PartitionSpec, StructLike>> partitions) {
    Map<Integer, Comparator<StructLike>> comparatorsBySpecId = Maps.newHashMap();
    for (Map.Entry<String, Collection<DeleteFile>> entry : duplicates.asMap().entrySet()) {
      String referencedFile = entry.getKey();

      // validate that each file matches the expected partition
      Pair<PartitionSpec, StructLike> partition = partitions.get(referencedFile);
      Long sequenceNumber = Iterables.getFirst(entry.getValue(), null).dataSequenceNumber();
      PartitionSpec spec = partition.first();
      StructLike tuple = partition.second();
      Comparator<StructLike> comparator =
          comparatorsBySpecId.computeIfAbsent(
              spec.specId(), id -> Comparators.forType(spec.partitionType()));

      for (DeleteFile dv : entry.getValue()) {
        Preconditions.checkArgument(
            Objects.equals(sequenceNumber, dv.dataSequenceNumber()),
            "Cannot merge DVs, mismatched sequence numbers (%s, %s) for %s",
            sequenceNumber,
            dv.dataSequenceNumber(),
            referencedFile);

        Preconditions.checkArgument(
            spec.specId() == dv.specId(),
            "Cannot merge DVs, mismatched partition specs (%s, %s) for %s",
            spec.specId(),
            dv.specId(),
            referencedFile);

        Preconditions.checkArgument(
            comparator.compare(tuple, dv.partition()) == 0,
            "Cannot merge DVs, mismatched partition tuples (%s, %s) for %s",
            tuple,
            dv.partition(),
            referencedFile);
      }
    }
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
      DeleteFile[] duplicateDVs, FileIO io, ExecutorService pool) {
    // Read all duplicate DVs in parallel
    PositionDeleteIndex[] duplicatedDVPositions = new PositionDeleteIndex[duplicateDVs.length];
    Tasks.range(duplicatedDVPositions.length)
        .executeWith(pool)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .run(i -> duplicatedDVPositions[i] = readDV(duplicateDVs[i], io));

    Map<String, PositionDeleteIndex> mergedDVs = Maps.newHashMap();
    for (int i = 0; i < duplicatedDVPositions.length; i++) {
      DeleteFile dv = duplicateDVs[i];
      PositionDeleteIndex previousDV = mergedDVs.get(duplicateDVs[i].referencedDataFile());
      if (previousDV != null) {
        previousDV.merge(duplicatedDVPositions[i]);
      } else {
        mergedDVs.put(dv.referencedDataFile(), duplicatedDVPositions[i]);
      }
    }

    return mergedDVs;
  }

  // Produces a single Puffin file containing the merged DVs
  private static List<DeleteFile> writeDVs(
      Map<String, PositionDeleteIndex> mergedIndexByFile,
      FileIO fileIO,
      String dvOutputLocation,
      Map<String, Pair<PartitionSpec, StructLike>> partitions) {
    OutputFile dvOutputFile = fileIO.newOutputFile(dvOutputLocation);
    try (DVFileWriter dvFileWriter = new BaseDVFileWriter(() -> dvOutputFile, path -> null)) {
      for (Map.Entry<String, PositionDeleteIndex> entry : mergedIndexByFile.entrySet()) {
        String referencedLocation = entry.getKey();
        PositionDeleteIndex mergedPositions = entry.getValue();
        Pair<PartitionSpec, StructLike> partition = partitions.get(referencedLocation);
        dvFileWriter.delete(
            referencedLocation, mergedPositions, partition.first(), partition.second());
      }
      dvFileWriter.close();
      return dvFileWriter.result().deleteFiles();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
