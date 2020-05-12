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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;

import static org.apache.iceberg.expressions.Expressions.alwaysTrue;

/**
 * Reader for manifest files.
 * <p>
 * Create readers using {@link ManifestFiles#read(ManifestFile, FileIO, Map)}.
 */
public class ManifestReader extends CloseableGroup implements CloseableIterable<DataFile> {
  static final ImmutableList<String> ALL_COLUMNS = ImmutableList.of("*");
  static final ImmutableList<String> CHANGE_COLUMNS = ImmutableList.of(
      "file_path", "file_format", "partition", "record_count", "file_size_in_bytes");
  private static final Set<String> STATS_COLUMNS = Sets.newHashSet(
      "value_counts", "null_value_counts", "lower_bounds", "upper_bounds");

  /**
   * Returns a new {@link ManifestReader} for an {@link InputFile}.
   * <p>
   * <em>Note:</em> Callers should use {@link ManifestFiles#read(ManifestFile, FileIO, Map)} to ensure that
   * manifest entries with partial metadata can inherit missing properties from the manifest metadata.
   *
   * @param file an InputFile
   * @return a manifest reader
   * @deprecated will be removed in 0.9.0; use {@link ManifestFiles#read(ManifestFile, FileIO, Map)} instead.
   */
  @Deprecated
  public static ManifestReader read(InputFile file) {
    return read(file, null);
  }

  /**
   * Returns a new {@link ManifestReader} for an {@link InputFile}.
   *
   * @param file an InputFile
   * @param specLookup a function to look up the manifest's partition spec by ID
   * @return a manifest reader
   * @deprecated will be removed in 0.9.0; use {@link ManifestFiles#read(ManifestFile, FileIO, Map)} instead.
   */
  @Deprecated
  public static ManifestReader read(InputFile file, Function<Integer, PartitionSpec> specLookup) {
    return new ManifestReader(file, specLookup, InheritableMetadataFactory.empty());
  }

  private final InputFile file;
  private final InheritableMetadata inheritableMetadata;
  private final Map<String, String> metadata;
  private final PartitionSpec spec;
  private final Schema fileSchema;

  // updated by configuration methods
  private Expression partFilter = alwaysTrue();
  private Expression rowFilter = alwaysTrue();
  private Schema fileProjection = null;
  private Collection<String> columns = null;
  private boolean caseSensitive = true;

  // lazily initialized
  private List<ManifestEntry> cachedAdds = null;
  private List<ManifestEntry> cachedDeletes = null;
  private Evaluator lazyEvaluator = null;
  private InclusiveMetricsEvaluator lazyMetricsEvaluator = null;

  ManifestReader(InputFile file, Map<Integer, PartitionSpec> specsById,
                 InheritableMetadata inheritableMetadata) {
    this(file, specsById != null ? specsById::get : null, inheritableMetadata);
  }

  private ManifestReader(InputFile file, Function<Integer, PartitionSpec> specLookup,
                         InheritableMetadata inheritableMetadata) {
    this.file = file;
    this.inheritableMetadata = inheritableMetadata;

    try {
      try (AvroIterable<ManifestEntry> headerReader = Avro.read(file)
          .project(ManifestEntry.getSchema(Types.StructType.of()).select("status"))
          .build()) {
        this.metadata = headerReader.getMetadata();
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }

    int specId = TableMetadata.INITIAL_SPEC_ID;
    String specProperty = metadata.get("partition-spec-id");
    if (specProperty != null) {
      specId = Integer.parseInt(specProperty);
    }

    if (specLookup != null) {
      this.spec = specLookup.apply(specId);
    } else {
      Schema schema = SchemaParser.fromJson(metadata.get("schema"));
      this.spec = PartitionSpecParser.fromJsonFields(schema, specId, metadata.get("partition-spec"));
    }

    this.fileSchema = new Schema(DataFile.getType(spec.partitionType()).fields());
  }

  public InputFile file() {
    return file;
  }

  public Schema schema() {
    return fileSchema;
  }

  public PartitionSpec spec() {
    return spec;
  }

  public ManifestReader select(Collection<String> newColumns) {
    Preconditions.checkState(fileProjection == null,
        "Cannot select columns using both select(String...) and project(Schema)");
    this.columns = newColumns;
    return this;
  }

  public ManifestReader project(Schema newFileProjection) {
    Preconditions.checkState(columns == null,
        "Cannot select columns using both select(String...) and project(Schema)");
    this.fileProjection = newFileProjection;
    return this;
  }

  public ManifestReader filterPartitions(Expression expr) {
    this.partFilter = Expressions.and(partFilter, expr);
    return this;
  }

  public ManifestReader filterRows(Expression expr) {
    this.rowFilter = Expressions.and(rowFilter, expr);
    return this;
  }

  public ManifestReader caseSensitive(boolean isCaseSensitive) {
    this.caseSensitive = isCaseSensitive;
    return this;
  }

  public List<ManifestEntry> addedFiles() {
    if (cachedAdds == null) {
      cacheChanges();
    }
    return cachedAdds;
  }

  public List<ManifestEntry> deletedFiles() {
    if (cachedDeletes == null) {
      cacheChanges();
    }
    return cachedDeletes;
  }

  private void cacheChanges() {
    List<ManifestEntry> adds = Lists.newArrayList();
    List<ManifestEntry> deletes = Lists.newArrayList();

    try (CloseableIterable<ManifestEntry> entries = open(fileSchema.select(CHANGE_COLUMNS))) {
      for (ManifestEntry entry : entries) {
        switch (entry.status()) {
          case ADDED:
            adds.add(entry.copyWithoutStats());
            break;
          case DELETED:
            deletes.add(entry.copyWithoutStats());
            break;
          default:
        }
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to close manifest entries");
    }

    this.cachedAdds = adds;
    this.cachedDeletes = deletes;
  }

  CloseableIterable<ManifestEntry> entries() {
    if ((rowFilter != null && rowFilter != Expressions.alwaysTrue()) ||
        (partFilter != null && partFilter != Expressions.alwaysTrue())) {
      Evaluator evaluator = evaluator();
      InclusiveMetricsEvaluator metricsEvaluator = metricsEvaluator();

      // ensure stats columns are present for metrics evaluation
      boolean requireStatsProjection = requireStatsProjection(rowFilter, columns);
      Collection<String> projectColumns = requireStatsProjection ? withStatsColumns(columns) : columns;

      return CloseableIterable.filter(
          open(projection(fileSchema, fileProjection, projectColumns, caseSensitive)),
          entry -> entry != null &&
              evaluator.eval(entry.file().partition()) &&
              metricsEvaluator.eval(entry.file()));
    } else {
      return open(projection(fileSchema, fileProjection, columns, caseSensitive));
    }
  }

  private CloseableIterable<ManifestEntry> open(Schema projection) {
    FileFormat format = FileFormat.fromFileName(file.location());
    Preconditions.checkArgument(format != null, "Unable to determine format of manifest: %s", file);

    switch (format) {
      case AVRO:
        AvroIterable<ManifestEntry> reader = Avro.read(file)
            .project(ManifestEntry.wrapFileSchema(projection.asStruct()))
            .rename("manifest_entry", GenericManifestEntry.class.getName())
            .rename("partition", PartitionData.class.getName())
            .rename("r102", PartitionData.class.getName())
            .rename("data_file", GenericDataFile.class.getName())
            .rename("r2", GenericDataFile.class.getName())
            .classLoader(GenericManifestFile.class.getClassLoader())
            .reuseContainers()
            .build();

        addCloseable(reader);

        return CloseableIterable.transform(reader, inheritableMetadata::apply);

      default:
        throw new UnsupportedOperationException("Invalid format for manifest file: " + format);
    }
  }

  CloseableIterable<ManifestEntry> liveEntries() {
    return CloseableIterable.filter(entries(),
        entry -> entry != null && entry.status() != ManifestEntry.Status.DELETED);
  }

  /**
   * @return an Iterator of DataFile. Makes defensive copies of files before returning
   */
  @Override
  public Iterator<DataFile> iterator() {
    if (dropStats(rowFilter, columns)) {
      return CloseableIterable.transform(liveEntries(), e -> e.file().copyWithoutStats()).iterator();
    } else {
      return CloseableIterable.transform(liveEntries(), e -> e.file().copy()).iterator();
    }
  }

  private static Schema projection(Schema schema, Schema project, Collection<String> columns, boolean caseSensitive) {
    if (columns != null) {
      if (caseSensitive) {
        return schema.select(columns);
      } else {
        return schema.caseInsensitiveSelect(columns);
      }
    } else if (project != null) {
      return project;
    }

    return schema;
  }

  private Evaluator evaluator() {
    if (lazyEvaluator == null) {
      Expression projected = Projections.inclusive(spec, caseSensitive).project(rowFilter);
      Expression finalPartFilter = Expressions.and(projected, partFilter);
      if (finalPartFilter != null) {
        this.lazyEvaluator = new Evaluator(spec.partitionType(), finalPartFilter, caseSensitive);
      } else {
        this.lazyEvaluator = new Evaluator(spec.partitionType(), Expressions.alwaysTrue(), caseSensitive);
      }
    }
    return lazyEvaluator;
  }

  private InclusiveMetricsEvaluator metricsEvaluator() {
    if (lazyMetricsEvaluator == null) {
      if (rowFilter != null) {
        this.lazyMetricsEvaluator = new InclusiveMetricsEvaluator(
            spec.schema(), rowFilter, caseSensitive);
      } else {
        this.lazyMetricsEvaluator = new InclusiveMetricsEvaluator(
            spec.schema(), Expressions.alwaysTrue(), caseSensitive);
      }
    }
    return lazyMetricsEvaluator;
  }

  private static boolean requireStatsProjection(Expression rowFilter, Collection<String> columns) {
    // Make sure we have all stats columns for metrics evaluator
    return rowFilter != Expressions.alwaysTrue() &&
        !columns.containsAll(ManifestReader.ALL_COLUMNS) &&
        !columns.containsAll(STATS_COLUMNS);
  }

  static boolean dropStats(Expression rowFilter, Collection<String> columns) {
    // Make sure we only drop all stats if we had projected all stats
    // We do not drop stats even if we had partially added some stats columns
    return rowFilter != Expressions.alwaysTrue() &&
        !columns.containsAll(ManifestReader.ALL_COLUMNS) &&
        Sets.intersection(Sets.newHashSet(columns), STATS_COLUMNS).isEmpty();
  }

  private static Collection<String> withStatsColumns(Collection<String> columns) {
    if (columns.containsAll(ManifestReader.ALL_COLUMNS)) {
      return columns;
    } else {
      List<String> projectColumns = Lists.newArrayList(columns);
      projectColumns.addAll(STATS_COLUMNS); // order doesn't matter
      return projectColumns;
    }
  }
}
