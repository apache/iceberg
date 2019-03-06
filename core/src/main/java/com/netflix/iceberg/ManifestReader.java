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

package com.netflix.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.netflix.iceberg.avro.Avro;
import com.netflix.iceberg.avro.AvroIterable;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.Projections;
import com.netflix.iceberg.io.CloseableGroup;
import com.netflix.iceberg.io.CloseableIterable;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.netflix.iceberg.ManifestEntry.Status.DELETED;
import static com.netflix.iceberg.expressions.Expressions.alwaysTrue;

/**
 * Reader for manifest files.
 * <p>
 * Readers are created using the builder from {@link #read(InputFile)}.
 */
public class ManifestReader extends CloseableGroup implements Filterable<FilteredManifest> {
  private static final Logger LOG = LoggerFactory.getLogger(ManifestReader.class);

  private static final List<String> ALL_COLUMNS = Lists.newArrayList("*");
  private static final List<String> CHANGE_COLUNNS = Lists.newArrayList(
      "file_path", "file_format", "partition", "record_count", "file_size_in_bytes");

  /**
   * Returns a new {@link ManifestReader} for an {@link InputFile}.
   *
   * @param file an InputFile
   * @return a manifest reader
   */
  public static ManifestReader read(InputFile file) {
    return new ManifestReader(file);
  }

  private final InputFile file;
  private final Map<String, String> metadata;
  private final PartitionSpec spec;
  private final Schema schema;

  // lazily initialized
  private List<ManifestEntry> adds = null;
  private List<ManifestEntry> deletes = null;

  private ManifestReader(InputFile file) {
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
    this.schema = SchemaParser.fromJson(metadata.get("schema"));
    int specId = TableMetadata.INITIAL_SPEC_ID;
    String specProperty = metadata.get("partition-spec-id");
    if (specProperty != null) {
      specId = Integer.parseInt(specProperty);
    }
    this.spec = PartitionSpecParser.fromJsonFields(schema, specId, metadata.get("partition-spec"));
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
    return new FilteredManifest(this, alwaysTrue(), alwaysTrue(), Lists.newArrayList(columns));
  }

  @Override
  public FilteredManifest filterPartitions(Expression expr) {
    return new FilteredManifest(this, expr, alwaysTrue(), ALL_COLUMNS);
  }

  @Override
  public FilteredManifest filterRows(Expression expr) {
    return new FilteredManifest(this, Projections.inclusive(spec).project(expr), expr, ALL_COLUMNS);
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
