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
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.expressions.Expressions.alwaysTrue;

/**
 * Reader for manifest files.
 * <p>
 * The preferable way to create readers is using {@link #read(ManifestFile, FileIO, Map)} as
 * it allows entries to inherit manifest metadata such as snapshot id.
 */
public class ManifestReader extends CloseableGroup implements Filterable<FilteredManifest> {
  private static final Logger LOG = LoggerFactory.getLogger(ManifestReader.class);

  static final ImmutableList<String> ALL_COLUMNS = ImmutableList.of("*");
  static final ImmutableList<String> CHANGE_COLUMNS = ImmutableList.of(
      "file_path", "file_format", "partition", "record_count", "file_size_in_bytes");
  static final ImmutableList<String> CHANGE_WITH_STATS_COLUMNS = ImmutableList.<String>builder()
      .addAll(CHANGE_COLUMNS)
      .add("value_counts", "null_value_counts", "lower_bounds", "upper_bounds")
      .build();

  /**
   * Returns a new {@link ManifestReader} for an {@link InputFile}.
   * <p>
   * <em>Note:</em> Most callers should use {@link #read(ManifestFile, FileIO, Map)} to ensure that
   * manifest entries with partial metadata can inherit missing properties from the manifest metadata.
   * <p>
   * <em>Note:</em> Most callers should use {@link #read(InputFile, Map)} if all manifest entries
   * contain full metadata and they want to ensure that the schema used by filters is the latest
   * table schema. This should be used only when reading a manifest without filters.
   *
   * @param file an InputFile
   * @return a manifest reader
   */
  public static ManifestReader read(InputFile file) {
    return new ManifestReader(file, null, InheritableMetadataFactory.empty());
  }

  /**
   * Returns a new {@link ManifestReader} for an {@link InputFile}.
   * <p>
   * <em>Note:</em> Most callers should use {@link #read(ManifestFile, FileIO, Map)} to ensure that
   * manifest entries with partial metadata can inherit missing properties from the manifest metadata.
   *
   * @param file an InputFile
   * @param specsById a Map from spec ID to partition spec
   * @return a manifest reader
   */
  public static ManifestReader read(InputFile file, Map<Integer, PartitionSpec> specsById) {
    return new ManifestReader(file, specsById, InheritableMetadataFactory.empty());
  }

  /**
   * Returns a new {@link ManifestReader} for a {@link ManifestFile}.
   * <p>
   * <em>Note:</em> Most callers should use {@link #read(ManifestFile, FileIO, Map)} to ensure
   * the schema used by filters is the latest table schema. This should be used only when reading
   * a manifest without filters.
   *
   * @param manifest a ManifestFile
   * @param io a FileIO
   * @return a manifest reader
   */
  public static ManifestReader read(ManifestFile manifest, FileIO io) {
    return read(manifest, io, null);
  }

  /**
   * Returns a new {@link ManifestReader} for a {@link ManifestFile}.
   *
   * @param manifest a ManifestFile
   * @param io a FileIO
   * @param specsById a Map from spec ID to partition spec
   * @return a manifest reader
   */
  public static ManifestReader read(ManifestFile manifest, FileIO io, Map<Integer, PartitionSpec> specsById) {
    InputFile file = io.newInputFile(manifest.path());
    InheritableMetadata inheritableMetadata = InheritableMetadataFactory.fromManifest(manifest);
    return new ManifestReader(file, specsById, inheritableMetadata);
  }

  private final InputFile file;
  private final InheritableMetadata inheritableMetadata;
  private final Map<String, String> metadata;
  private final PartitionSpec spec;
  private final Schema fileSchema;

  // lazily initialized
  private List<ManifestEntry> cachedAdds = null;
  private List<ManifestEntry> cachedDeletes = null;

  private ManifestReader(InputFile file, Map<Integer, PartitionSpec> specsById,
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

    if (specsById != null) {
      this.spec = specsById.get(specId);
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

  @Override
  public FilteredManifest select(Collection<String> columns) {
    return new FilteredManifest(this, alwaysTrue(), alwaysTrue(), fileSchema, columns, true);
  }

  @Override
  public FilteredManifest project(Schema fileProjection) {
    return new FilteredManifest(this, alwaysTrue(), alwaysTrue(), fileProjection, ALL_COLUMNS, true);
  }

  @Override
  public FilteredManifest filterPartitions(Expression expr) {
    return new FilteredManifest(this, expr, alwaysTrue(), fileSchema, ALL_COLUMNS, true);
  }

  @Override
  public FilteredManifest filterRows(Expression expr) {
    return new FilteredManifest(this, alwaysTrue(), expr, fileSchema, ALL_COLUMNS, true);
  }

  @Override
  public FilteredManifest caseSensitive(boolean caseSensitive) {
    return new FilteredManifest(this, alwaysTrue(), alwaysTrue(), fileSchema, ALL_COLUMNS, caseSensitive);
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

    try (CloseableIterable<ManifestEntry> entries = entries(fileSchema.select(CHANGE_COLUMNS))) {
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
    return entries(fileSchema);
  }

  CloseableIterable<ManifestEntry> entries(Schema fileProjection) {
    FileFormat format = FileFormat.fromFileName(file.location());
    Preconditions.checkArgument(format != null, "Unable to determine format of manifest: %s", file);

    switch (format) {
      case AVRO:
        AvroIterable<ManifestEntry> reader = Avro.read(file)
            .project(ManifestEntry.wrapFileSchema(fileProjection.asStruct()))
            .rename("manifest_entry", ManifestEntry.class.getName())
            .rename("partition", PartitionData.class.getName())
            .rename("r102", PartitionData.class.getName())
            .rename("data_file", GenericDataFile.class.getName())
            .rename("r2", GenericDataFile.class.getName())
            .classLoader(ManifestEntry.class.getClassLoader())
            .reuseContainers()
            .build();

        addCloseable(reader);

        return CloseableIterable.transform(reader, inheritableMetadata::apply);

      default:
        throw new UnsupportedOperationException("Invalid format for manifest file: " + format);
    }
  }

  @Override
  public Iterator<DataFile> iterator() {
    return iterator(fileSchema);
  }

  // visible for use by PartialManifest
  Iterator<DataFile> iterator(Schema fileProjection) {
    return Iterables.transform(Iterables.filter(
        entries(fileProjection),
        entry -> entry.status() != ManifestEntry.Status.DELETED),
        ManifestEntry::file).iterator();
  }

}
