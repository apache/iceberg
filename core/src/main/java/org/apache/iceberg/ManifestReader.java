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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionSet;

import static org.apache.iceberg.expressions.Expressions.alwaysTrue;

/**
 * Base reader for data and delete manifest files.
 *
 * @param <F> The Java class of files returned by this reader.
 */
public class ManifestReader<F extends ContentFile<F>>
    extends CloseableGroup implements CloseableIterable<F> {
  static final ImmutableList<String> ALL_COLUMNS = ImmutableList.of("*");

  private static final Set<String> STATS_COLUMNS = ImmutableSet.of(
      "value_counts", "null_value_counts", "nan_value_counts", "lower_bounds", "upper_bounds", "record_count");

  protected enum FileType {
    DATA_FILES(GenericDataFile.class.getName()),
    DELETE_FILES(GenericDeleteFile.class.getName());

    private final String fileClass;

    FileType(String fileClass) {
      this.fileClass = fileClass;
    }

    private String fileClass() {
      return fileClass;
    }
  }

  private final InputFile file;
  private final InheritableMetadata inheritableMetadata;
  private final FileType content;
  private final Map<String, String> metadata;
  private final PartitionSpec spec;
  private final Schema fileSchema;

  // updated by configuration methods
  private PartitionSet partitionSet = null;
  private Expression partFilter = alwaysTrue();
  private Expression rowFilter = alwaysTrue();
  private Schema fileProjection = null;
  private Collection<String> columns = null;
  private boolean caseSensitive = true;

  // lazily initialized
  private Evaluator lazyEvaluator = null;
  private InclusiveMetricsEvaluator lazyMetricsEvaluator = null;

  protected ManifestReader(InputFile file, Map<Integer, PartitionSpec> specsById,
                           InheritableMetadata inheritableMetadata, FileType content) {
    this.file = file;
    this.inheritableMetadata = inheritableMetadata;
    this.content = content;

    try {
      try (AvroIterable<ManifestEntry<F>> headerReader = Avro.read(file)
          .project(ManifestEntry.getSchema(Types.StructType.of()).select("status"))
          .classLoader(GenericManifestEntry.class.getClassLoader())
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

    if (specsById != null) {
      this.spec = specsById.get(specId);
    } else {
      Schema schema = SchemaParser.fromJson(metadata.get("schema"));
      this.spec = PartitionSpecParser.fromJsonFields(schema, specId, metadata.get("partition-spec"));
    }

    this.fileSchema = new Schema(DataFile.getType(spec.partitionType()).fields());
  }

  public boolean isDeleteManifestReader() {
    return content == FileType.DELETE_FILES;
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

  public ManifestReader<F> select(Collection<String> newColumns) {
    Preconditions.checkState(fileProjection == null,
        "Cannot select columns using both select(String...) and project(Schema)");
    this.columns = newColumns;
    return this;
  }

  public ManifestReader<F> project(Schema newFileProjection) {
    Preconditions.checkState(columns == null,
        "Cannot select columns using both select(String...) and project(Schema)");
    this.fileProjection = newFileProjection;
    return this;
  }

  public ManifestReader<F> filterPartitions(Expression expr) {
    this.partFilter = Expressions.and(partFilter, expr);
    return this;
  }

  public ManifestReader<F> filterPartitions(PartitionSet partitions) {
    this.partitionSet = partitions;
    return this;
  }

  public ManifestReader<F> filterRows(Expression expr) {
    this.rowFilter = Expressions.and(rowFilter, expr);
    return this;
  }

  public ManifestReader<F> caseSensitive(boolean isCaseSensitive) {
    this.caseSensitive = isCaseSensitive;
    return this;
  }

  CloseableIterable<ManifestEntry<F>> entries() {
    if ((rowFilter != null && rowFilter != Expressions.alwaysTrue()) ||
        (partFilter != null && partFilter != Expressions.alwaysTrue()) ||
        (partitionSet != null && !partitionSet.isEmpty())) {
      Evaluator evaluator = evaluator();
      InclusiveMetricsEvaluator metricsEvaluator = metricsEvaluator();

      // ensure stats columns are present for metrics evaluation
      boolean requireStatsProjection = requireStatsProjection(rowFilter, columns);
      Collection<String> projectColumns = requireStatsProjection ? withStatsColumns(columns) : columns;

      return CloseableIterable.filter(
          open(projection(fileSchema, fileProjection, projectColumns, caseSensitive)),
          entry -> entry != null &&
              evaluator.eval(entry.file().partition()) &&
              metricsEvaluator.eval(entry.file()) &&
              inPartitionSet(entry.file()));
    } else {
      return open(projection(fileSchema, fileProjection, columns, caseSensitive));
    }
  }

  private boolean inPartitionSet(F fileToCheck) {
    return partitionSet == null || partitionSet.contains(fileToCheck.specId(), fileToCheck.partition());
  }

  private CloseableIterable<ManifestEntry<F>> open(Schema projection) {
    FileFormat format = FileFormat.fromFileName(file.location());
    Preconditions.checkArgument(format != null, "Unable to determine format of manifest: %s", file);

    List<Types.NestedField> fields = Lists.newArrayList();
    fields.addAll(projection.asStruct().fields());
    fields.add(MetadataColumns.ROW_POSITION);

    switch (format) {
      case AVRO:
        AvroIterable<ManifestEntry<F>> reader = Avro.read(file)
            .project(ManifestEntry.wrapFileSchema(Types.StructType.of(fields)))
            .rename("manifest_entry", GenericManifestEntry.class.getName())
            .rename("partition", PartitionData.class.getName())
            .rename("r102", PartitionData.class.getName())
            .rename("data_file", content.fileClass())
            .rename("r2", content.fileClass())
            .classLoader(GenericManifestEntry.class.getClassLoader())
            .reuseContainers()
            .build();

        addCloseable(reader);

        return CloseableIterable.transform(reader, inheritableMetadata::apply);

      default:
        throw new UnsupportedOperationException("Invalid format for manifest file: " + format);
    }
  }

  CloseableIterable<ManifestEntry<F>> liveEntries() {
    return CloseableIterable.filter(entries(),
        entry -> entry != null && entry.status() != ManifestEntry.Status.DELETED);
  }

  /**
   * @return an Iterator of DataFile. Makes defensive copies of files before returning
   */
  @Override
  public CloseableIterator<F> iterator() {
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
        columns != null &&
        !columns.containsAll(ManifestReader.ALL_COLUMNS) &&
        !columns.containsAll(STATS_COLUMNS);
  }

  static boolean dropStats(Expression rowFilter, Collection<String> columns) {
    // Make sure we only drop all stats if we had projected all stats
    // We do not drop stats even if we had partially added some stats columns, except for record_count column.
    // Since we don't want to keep stats map which could be huge in size just because we select record_count, which
    // is a primitive type.
    if (rowFilter != Expressions.alwaysTrue() && columns != null &&
        !columns.containsAll(ManifestReader.ALL_COLUMNS)) {
      Set<String> intersection = Sets.intersection(Sets.newHashSet(columns), STATS_COLUMNS);
      return intersection.isEmpty() || intersection.equals(Sets.newHashSet("record_count"));
    }
    return false;
  }

  static List<String> withStatsColumns(Collection<String> columns) {
    if (columns.containsAll(ManifestReader.ALL_COLUMNS)) {
      return Lists.newArrayList(columns);
    } else {
      List<String> projectColumns = Lists.newArrayList(columns);
      projectColumns.addAll(STATS_COLUMNS); // order doesn't matter
      return projectColumns;
    }
  }
}
