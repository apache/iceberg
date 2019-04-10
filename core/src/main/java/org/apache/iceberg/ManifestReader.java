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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.ManifestEntry.Status.DELETED;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;

/**
 * Reader for manifest files.
 * <p>
 * Readers are created using the builder from {@link #read(InputFile, Function)}.
 */
public class ManifestReader extends CloseableGroup implements Filterable<FilteredManifest> {
  private static final Logger LOG = LoggerFactory.getLogger(ManifestReader.class);

  private static final List<String> ALL_COLUMNS = Lists.newArrayList("*");
  private static final List<String> CHANGE_COLUNNS = Lists.newArrayList(
      "file_path", "file_format", "partition", "record_count", "file_size_in_bytes");

  // Visible for testing
  static ManifestReader read(InputFile file) {
    return new ManifestReader(file, null);
  }

  /**
   * Returns a new {@link ManifestReader} for an {@link InputFile}.
   *
   * @param file an InputFile
   * @param specLookup a function to look up the manifest's partition spec by ID
   * @return a manifest reader
   */
  public static ManifestReader read(InputFile file, Function<Integer, PartitionSpec> specLookup) {
    return new ManifestReader(file, specLookup);
  }

  private final InputFile file;
  private final Map<String, String> metadata;
  private final PartitionSpec spec;
  private final Schema schema;
  private final boolean caseSensitive;

  // lazily initialized
  private List<ManifestEntry> adds = null;
  private List<ManifestEntry> deletes = null;

  private ManifestReader(InputFile file, Function<Integer, PartitionSpec> specLookup) {
    this.file = file;

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
      this.schema = spec.schema();
    } else {
      this.schema = SchemaParser.fromJson(metadata.get("schema"));
      this.spec = PartitionSpecParser.fromJsonFields(schema, specId, metadata.get("partition-spec"));
    }

    this.caseSensitive = true;
  }

  private ManifestReader(InputFile file, Map<String, String> metadata,
                         PartitionSpec spec, Schema schema, boolean caseSensitive) {
    this.file = file;
    this.metadata = metadata;
    this.spec = spec;
    this.schema = schema;
    this.caseSensitive = caseSensitive;
  }

  /**
   * Returns a new {@link ManifestReader} that, if filtered via {@link #select(java.util.Collection)},
   * {@link #filterPartitions(Expression)} or {@link #filterRows(Expression)}, will apply the specified
   * case sensitivity for column name matching.
   *
   * @param caseSensitive whether column name matching should have case sensitivity
   * @return a manifest reader with case sensitivity as stated
   */
  public ManifestReader caseSensitive(boolean caseSensitive) {
    return new ManifestReader(file, metadata, spec, schema, caseSensitive);
  }

  public InputFile file() {
    return file;
  }

  public Schema schema() {
    return schema;
  }

  public PartitionSpec spec() {
    return spec;
  }

  @Override
  public Iterator<DataFile> iterator() {
    return iterator(alwaysTrue(), ALL_COLUMNS);
  }

  @Override
  public FilteredManifest select(Collection<String> columns) {
    return new FilteredManifest(this, alwaysTrue(), alwaysTrue(), Lists.newArrayList(columns), caseSensitive);
  }

  @Override
  public FilteredManifest filterPartitions(Expression expr) {
    return new FilteredManifest(this, expr, alwaysTrue(), ALL_COLUMNS, caseSensitive);
  }

  @Override
  public FilteredManifest filterRows(Expression expr) {
    return new FilteredManifest(this,
      Projections.inclusive(spec, caseSensitive).project(expr),
      expr,
      ALL_COLUMNS,
      caseSensitive);
  }

  public List<ManifestEntry> addedFiles() {
    if (adds == null) {
      cacheChanges();
    }
    return adds;
  }

  public List<ManifestEntry> deletedFiles() {
    if (deletes == null) {
      cacheChanges();
    }
    return deletes;
  }

  private void cacheChanges() {
    List<ManifestEntry> adds = Lists.newArrayList();
    List<ManifestEntry> deletes = Lists.newArrayList();

    for (ManifestEntry entry : entries(CHANGE_COLUNNS)) {
      switch (entry.status()) {
        case ADDED:
          adds.add(entry.copy());
          break;
        case DELETED:
          deletes.add(entry.copy());
          break;
        default:
      }
    }

    this.adds = adds;
    this.deletes = deletes;
  }

  CloseableIterable<ManifestEntry> entries() {
    return entries(ALL_COLUMNS);
  }

  CloseableIterable<ManifestEntry> entries(Collection<String> columns) {
    FileFormat format = FileFormat.fromFileName(file.location());
    Preconditions.checkArgument(format != null, "Unable to determine format of manifest: " + file);

    Schema schema = ManifestEntry.projectSchema(spec.partitionType(), columns);
    switch (format) {
      case AVRO:
        AvroIterable<ManifestEntry> reader = Avro.read(file)
            .project(schema)
            .rename("manifest_entry", ManifestEntry.class.getName())
            .rename("partition", PartitionData.class.getName())
            .rename("r102", PartitionData.class.getName())
            .rename("data_file", GenericDataFile.class.getName())
            .rename("r2", GenericDataFile.class.getName())
            .reuseContainers()
            .build();

        addCloseable(reader);

        return reader;

      default:
        throw new UnsupportedOperationException("Invalid format for manifest file: " + format);
    }
  }

  // visible for use by PartialManifest
  Iterator<DataFile> iterator(Expression partFilter, Collection<String> columns) {
    return Iterables.transform(Iterables.filter(
        entries(columns),
        entry -> entry.status() != DELETED),
        ManifestEntry::file).iterator();
  }

}
