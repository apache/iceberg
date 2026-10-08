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
package org.apache.iceberg.formats;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.ColumnFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.VerticalSplitProjectionPlanner;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.Pair;

/**
 * Reads a {@link DataFile}, including the column files it references.
 *
 * <p>The physical files are resolved by location through the function supplied by the caller, so
 * the builder needs neither a {@code FileIO} nor an encryption manager. Readers that already
 * maintain a location to {@link InputFile} mapping for a task group can supply its lookup directly.
 *
 * @param <D> the output data type produced by the reader
 * @param <S> the type of the schema for the output data type
 */
public class DataFileReadBuilder<D, S> implements ReadBuilder<D, S> {
  private final DataFile file;
  private final Map<String, ReadBuilder<D, S>> builders;
  private final StitcherBuilder<D> stitcherBuilder;
  private Schema projection;
  private Expression filter = Expressions.alwaysTrue();
  private boolean caseSensitive = true;
  private List<Pair<Expression, Set<Integer>>> conjuncts;

  private DataFileReadBuilder(
      DataFile file, Class<? extends D> type, Function<String, InputFile> inputFiles) {
    this.file = file;
    this.builders = Maps.newLinkedHashMap();
    builders.put(file.location(), newReadBuilder(file.location(), file.format(), type, inputFiles));

    List<ColumnFile> columnFiles = file.columnFiles();
    if (columnFiles != null) {
      for (ColumnFile columnFile : columnFiles) {
        builders.put(
            columnFile.location(),
            newReadBuilder(columnFile.location(), columnFile.fileFormat(), type, inputFiles));
      }
    }

    this.stitcherBuilder = builders.size() > 1 ? StitcherRegistry.stitcherBuilder(type) : null;
  }

  /**
   * Returns a builder reading the given data file.
   *
   * @param file the data file to read
   * @param type the output type
   * @param inputFiles resolves the physical files of the read by location
   * @param <D> the type of data records the reader will produce
   * @param <S> the type of the output schema for the reader
   * @return a configurable builder for the given data file
   */
  public static <D, S> DataFileReadBuilder<D, S> read(
      DataFile file, Class<? extends D> type, Function<String, InputFile> inputFiles) {
    Preconditions.checkArgument(file != null, "Invalid data file: null");
    Preconditions.checkArgument(inputFiles != null, "Invalid input file provider: null");
    return new DataFileReadBuilder<>(file, type, inputFiles);
  }

  @Override
  public DataFileReadBuilder<D, S> split(long start, long length) {
    baseFileBuilder().split(start, length);
    return this;
  }

  @Override
  public DataFileReadBuilder<D, S> project(Schema schema) {
    this.projection = schema;
    this.conjuncts = null;
    return this;
  }

  // TODO gaborkaszab: figure out what to do here. Project the Schema to each writer similarly to
  // projection?
  @Override
  public DataFileReadBuilder<D, S> engineProjection(S schema) {
    baseFileBuilder().engineProjection(schema);
    return this;
  }

  @Override
  public DataFileReadBuilder<D, S> caseSensitive(boolean newCaseSensitive) {
    this.caseSensitive = newCaseSensitive;
    this.conjuncts = null;
    builders.values().forEach(builder -> builder.caseSensitive(newCaseSensitive));
    return this;
  }

  @Override
  public DataFileReadBuilder<D, S> filter(Expression newFilter) {
    this.filter = newFilter == null ? Expressions.alwaysTrue() : newFilter;
    this.conjuncts = null;
    return this;
  }

  @Override
  public DataFileReadBuilder<D, S> set(String key, String value) {
    builders.values().forEach(builder -> builder.set(key, value));
    return this;
  }

  @Override
  public DataFileReadBuilder<D, S> reuseContainers() {
    builders.values().forEach(ReadBuilder::reuseContainers);
    return this;
  }

  @Override
  public DataFileReadBuilder<D, S> recordsPerBatch(int rowsPerBatch) {
    builders.values().forEach(builder -> builder.recordsPerBatch(rowsPerBatch));
    return this;
  }

  @Override
  public DataFileReadBuilder<D, S> idToConstant(Map<Integer, ?> newIdToConstant) {
    baseFileBuilder().idToConstant(newIdToConstant);
    return this;
  }

  @Override
  public DataFileReadBuilder<D, S> withNameMapping(NameMapping newNameMapping) {
    builders.values().forEach(builder -> builder.withNameMapping(newNameMapping));
    return this;
  }

  @Override
  public CloseableIterable<D> build() {
    Preconditions.checkArgument(projection != null, "Invalid projection: null");

    Map<String, Schema> plan = VerticalSplitProjectionPlanner.plan(file, projection);
    Map<String, ReadBuilder<D, S>> readers = Maps.newLinkedHashMap(builders);
    projectReadBuilders(readers, plan);
    pushDownProjectedFilters(readers, plan);

    if (readers.size() == 1) {
      return Iterables.getOnlyElement(readers.values()).build();
    }

    List<Schema> parts = Lists.newArrayListWithCapacity(readers.size());
    List<CloseableIterable<D>> partReaders = Lists.newArrayListWithCapacity(readers.size());
    readers.forEach(
        (location, builder) -> {
          parts.add(plan.get(location));
          partReaders.add(builder.build());
        });

    return new RowAlignedStitchingIterable<>(partReaders, stitcherBuilder.build(projection, parts));
  }

  private void projectReadBuilders(
      Map<String, ReadBuilder<D, S>> readers, Map<String, Schema> plan) {
    readers.keySet().retainAll(plan.keySet());
    readers.forEach((location, builder) -> builder.project(plan.get(location)));
  }

  private void pushDownProjectedFilters(
      Map<String, ReadBuilder<D, S>> readers, Map<String, Schema> plan) {
    readers.forEach(
        (location, builder) -> {
          Expression fileFilter = projectFilter(TypeUtil.getProjectedIds(plan.get(location)));
          if (fileFilter != Expressions.alwaysTrue()) {
            builder.filter(fileFilter);
          }
        });
  }

  private Expression projectFilter(Set<Integer> fieldIds) {
    Expression result = Expressions.alwaysTrue();
    for (Pair<Expression, Set<Integer>> conjunct : conjuncts()) {
      if (fieldIds.containsAll(conjunct.second())) {
        result = Expressions.and(result, conjunct.first());
      }
    }

    return result;
  }

  private List<Pair<Expression, Set<Integer>>> conjuncts() {
    if (conjuncts == null) {
      List<Expression> expressions = Lists.newArrayList();
      collectConjuncts(filter, expressions);

      List<Pair<Expression, Set<Integer>>> result =
          Lists.newArrayListWithCapacity(expressions.size());
      for (Expression expression : expressions) {
        result.add(
            Pair.of(
                expression,
                Binder.boundReferences(
                    projection.asStruct(), ImmutableList.of(expression), caseSensitive)));
      }

      this.conjuncts = result;
    }

    return conjuncts;
  }

  private static void collectConjuncts(Expression expression, List<Expression> result) {
    if (expression instanceof And and) {
      collectConjuncts(and.left(), result);
      collectConjuncts(and.right(), result);
    } else {
      result.add(expression);
    }
  }

  private ReadBuilder<D, S> baseFileBuilder() {
    return builders.get(file.location());
  }

  private static <D, S> ReadBuilder<D, S> newReadBuilder(
      String location,
      FileFormat format,
      Class<? extends D> type,
      Function<String, InputFile> inputFiles) {
    InputFile inputFile = inputFiles.apply(location);
    Preconditions.checkArgument(
        inputFile != null, "Cannot find input file for location: %s", location);
    return FormatModelRegistry.readBuilder(format, type, inputFile);
  }
}
