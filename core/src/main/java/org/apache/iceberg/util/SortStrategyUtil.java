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

package org.apache.iceberg.util;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.StrictMetricsEvaluator;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.expressions.Expressions.equal;

public class SortStrategyUtil {
  private static final Logger LOG = LoggerFactory.getLogger(SortStrategyUtil.class);

  private SortStrategyUtil() {
  }

  /**
   * Measures the sortedness score for a given list of {@link DataFile}s.
   *
   * @param files List of {@link DataFile}s
   * @param sortOrder The sort order
   * @return A score between 0.0 and 1.0 to represent how well those data files are sorted
   * on the sort order.
   */
  public static double sortednessScore(List<DataFile> files, SortOrder sortOrder)  {
    if (files.size() == 1) {
      return 1.0;
    }

    List<Integer> sortColIndices = getSortColumnIndices(sortOrder);
    if (sortColIndices.isEmpty()) {
      // Cannot obtain indices of sort columns, will consider those files are completely unsorted
      return 0.0;
    }

    double totalScore = 0;
    // Compute and aggregate scores for all lowerBounds
    List<List<ByteBuffer>> lowerBounds = getColumnValueBounds(files, sortColIndices, true);
    for (List<ByteBuffer> point : lowerBounds) {
      totalScore += overlapScore(point, files, sortColIndices, sortOrder.schema());
    }
    // Compute and aggregate scores for all upperBounds
    List<List<ByteBuffer>> upperBounds = getColumnValueBounds(files, sortColIndices, false);
    for (List<ByteBuffer> point : upperBounds) {
      totalScore += overlapScore(point, files, sortColIndices, sortOrder.schema());
    }
    // Then normalize the result
    return totalScore / (2 * files.size());
  }

  /**
   * Removes files that are already sorted (having no overlap on sort column values with other files)
   * from a collection of file scans.
   *
   * @param fileScans The original collection of file scans
   * @param sortOrder The sort order
   * @return A collection of file scans after removing sorted ones from the original collection
   */
  public static Iterable<FileScanTask> removeSortedFiles(Iterable<FileScanTask> fileScans, SortOrder sortOrder) {
    List<Integer> sortColIndices = getSortColumnIndices(sortOrder);
    List<DataFile> dataFiles = FluentIterable.from(fileScans).stream()
        .map(FileScanTask::file)
        .collect(Collectors.toList());
    return FluentIterable.from(fileScans)
        .filter(f -> hasOverLap(f.file(), dataFiles, sortColIndices, sortOrder.schema()));
  }

  private static boolean hasOverLap(
      DataFile file,
      List<DataFile> dataFiles,
      List<Integer> sortColIndices,
      Schema sortSchema) {
    for (DataFile f : dataFiles) {
      if (!f.equals(file) && isOverlap(f, file, sortColIndices, sortSchema)) {
        return true;
      }
    }
    return false;
  }

  static List<Integer> getSortColumnIndices(SortOrder sortOrder) {
    List<Integer> sortColIndices = Lists.newArrayList();
    for (SortField sortField : sortOrder.fields()) {
      if (!sortField.transform().isIdentity()) {
        LOG.info("Cannot measure sortedness score on derived sort column {}",
            sortField.transform().dedupName());
        return ImmutableList.of();
      }
      sortColIndices.add(sortField.sourceId());
    }
    return sortColIndices;
  }

  // Counts how many data files from a given list have overlaps with a given data point
  static int overlapCount(
      List<ByteBuffer> point,
      List<DataFile> files,
      List<Integer> sortColIndices,
      Schema sortSchema) {
    int overLapCount = 0;
    for (DataFile file : files) {
      if (isOverlap(point, file, sortColIndices, sortSchema)) {
        overLapCount++;
      }
    }
    return overLapCount;
  }

  private static double overlapScore(
      List<ByteBuffer> point,
      List<DataFile> files,
      List<Integer> sortColIndices,
      Schema sortSchema) {
    double overlapScore = overlapCount(point, files, sortColIndices, sortSchema);
    return 1.0 - (overlapScore - 1.0) / (files.size() - 1.0);
  }

  // Check if a data point (x[1], x[2],..., x[k]) has overlap with a data file that has lower bounds of
  // (min[1], min[2],..., min[k]) and upper bounds of (max[1], max[2],..., max[k])
  // An overlap happens if for j going from 1 to k:
  // if x[j] < min[j] or x[j] > max[j] then return false
  // else if min[j <= x[j] <= max[j] and min[j] != max[j] and  then return true
  // else (meaning min[j] == x[j] == max[j]) continue with j + 1
  private static boolean isOverlap(
      List<ByteBuffer> point,
      DataFile file,
      List<Integer> sortColIndices,
      Schema sortSchema) {
    for (int j = 0; j < sortColIndices.size(); j++) {
      Types.NestedField col = sortSchema.findField(sortColIndices.get(j));
      if (point.get(j) == null || NaNUtil.isNaN(point.get(j))) {
        // null or NaN is considered to have overlap with everything
        return true;
      }
      Expression pred = equal(col.name(), Conversions.fromByteBuffer(col.type(), point.get(j)));
      // Check if min[j <= x[j] <= max[j]
      boolean inclusiveEvaluation = new InclusiveMetricsEvaluator(sortSchema, pred).eval(file);
      // Check if  min[j] == x[j] == max[j]
      boolean strictEvaluation = new StrictMetricsEvaluator(sortSchema, pred).eval(file);
      if (!inclusiveEvaluation) {
        // x[j] < min[j] or x[j] > max[j]
        return false;
      }
      if (!strictEvaluation) {
        // min[j <= x[j] <= max[j] and min[j] != max[j]
        return true;
      }
    }
    return true;
  }

  private static boolean isOverlap(
      DataFile file1,
      DataFile file2,
      List<Integer> sortColIndices,
      Schema sortSchema) {
    List<ByteBuffer> lowerEnd1 = getColumnValueBound(file1, sortColIndices, true);
    List<ByteBuffer> upperEnd1 = getColumnValueBound(file1, sortColIndices, false);
    List<ByteBuffer> lowerEnd2 = getColumnValueBound(file2, sortColIndices, true);
    List<ByteBuffer> upperEnd2 = getColumnValueBound(file2, sortColIndices, false);
    return isOverlap(lowerEnd1, file2, sortColIndices, sortSchema) ||
        isOverlap(upperEnd1, file2, sortColIndices, sortSchema) ||
        isOverlap(lowerEnd2, file1, sortColIndices, sortSchema) ||
        isOverlap(upperEnd2, file1, sortColIndices, sortSchema);
  }

  private static List<List<ByteBuffer>> getColumnValueBounds(
      List<DataFile> dataFiles,
      List<Integer> sortColIndices,
      boolean isLowerBound) {
    List<List<ByteBuffer>> results = Lists.newArrayList();
    for (DataFile file : dataFiles) {
      List<ByteBuffer> valueBound = getColumnValueBound(file, sortColIndices, isLowerBound);
      results.add(getColumnValueBound(file, sortColIndices, isLowerBound));
    }
    return results;
  }

  private static List<ByteBuffer> getColumnValueBound(
      DataFile dataFile,
      List<Integer> sortColIndices,
      boolean isLowerBound) {
    List<ByteBuffer> valueBound = Lists.newArrayList();
    for (Integer index : sortColIndices) {
      Map<Integer, ByteBuffer> valueMap = isLowerBound ? dataFile.lowerBounds() : dataFile.upperBounds();
      valueBound.add(valueMap != null ? valueMap.get(index) : null);
    }
    return valueBound;
  }
}
