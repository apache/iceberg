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
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.encryption.NativeEncryptionKeyMetadata;
import org.apache.iceberg.encryption.NativeEncryptionOutputFile;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types;

/**
 * Writer for manifest files.
 *
 * @param <F> Java class of files written to the manifest, either {@link DataFile} or {@link
 *     DeleteFile}.
 */
public abstract class ManifestWriter<F extends ContentFile<F>> implements FileAppender<F> {
  // stand-in for the current sequence number that will be assigned when the commit is successful
  // this is replaced when writing a manifest list by the ManifestFile wrapper
  static final long UNASSIGNED_SEQ = -1L;

  private final FileFormat format;
  private final OutputFile file;
  private final EncryptionKeyMetadata keyMetadata;
  private final int specId;
  private final FileAppender<ManifestEntry<F>> writer;
  private final Long snapshotId;
  private final GenericManifestEntry<F> reused;
  private final PartitionSummary stats;
  private final Long firstRowId;
  private final Map<String, String> writerProperties;

  private boolean closed = false;
  private int addedFiles = 0;
  private long addedRows = 0L;
  private int existingFiles = 0;
  private long existingRows = 0L;
  private int deletedFiles = 0;
  private long deletedRows = 0L;
  // v4: total number of entries appended via addEntry, across every status. Used by V4 writers to
  // populate the leaf manifest's record_count on the root-manifest content_entry row. Tracked
  // separately from the per-status counters above because v4 status counts (added/existing/deleted/
  // replaced/modified) are not all surfaced via this writer's accessors (e.g. MODIFIED gets folded
  // into existing in toManifestFile), and the on-disk record_count must equal the row count of
  // the leaf manifest file regardless.
  private int entriesWritten = 0;
  private Long minDataSequenceNumber = null;

  private ManifestWriter(
      PartitionSpec spec,
      EncryptedOutputFile file,
      Long snapshotId,
      Long firstRowId,
      Map<String, String> writerProperties) {
    this.format = FileFormat.fromFileName(file.encryptingOutputFile().location());
    this.file = outputFile(file);
    this.specId = spec.specId();
    this.writerProperties = writerProperties;
    this.writer = newAppender(spec, this.file);
    this.snapshotId = snapshotId;
    this.reused =
        new GenericManifestEntry<>(V1Metadata.entrySchema(spec.partitionType()).asStruct());
    this.stats = new PartitionSummary(spec);
    this.firstRowId = firstRowId;
    this.keyMetadata = file.keyMetadata();
  }

  protected abstract ManifestEntry<F> prepare(ManifestEntry<F> entry);

  protected abstract FileAppender<ManifestEntry<F>> newAppender(
      PartitionSpec spec, OutputFile outputFile);

  private OutputFile outputFile(EncryptedOutputFile encryptedFile) {
    // Casting to NativeEncryptionOutputFile actually makes the file rely on native encryption
    // rather than whole-file encryption.
    if (format == FileFormat.PARQUET
        && encryptedFile instanceof NativeEncryptionOutputFile nativeFile) {
      return nativeFile;
    }

    return encryptedFile.encryptingOutputFile();
  }

  protected FileFormat format() {
    return format;
  }

  protected Map<String, String> writerProperties() {
    return writerProperties;
  }

  protected ManifestContent content() {
    return ManifestContent.DATA;
  }

  protected Long writerSnapshotId() {
    return snapshotId;
  }

  protected GenericManifestEntry<F> reusedEntry() {
    return reused;
  }

  /** Total number of entries appended to this writer via {@link #addEntry}, across every status. */
  protected int entriesWritten() {
    return entriesWritten;
  }

  void addEntry(ManifestEntry<F> entry) {
    switch (entry.status()) {
      case ADDED:
        addedFiles += 1;
        addedRows += entry.file().recordCount();
        break;
      case EXISTING:
        existingFiles += 1;
        existingRows += entry.file().recordCount();
        break;
      case DELETED:
        deletedFiles += 1;
        deletedRows += entry.file().recordCount();
        break;
    }
    entriesWritten += 1;

    stats.update(entry.file().partition());

    if (entry.isLive()
        && entry.dataSequenceNumber() != null
        && (minDataSequenceNumber == null || entry.dataSequenceNumber() < minDataSequenceNumber)) {
      this.minDataSequenceNumber = entry.dataSequenceNumber();
    }

    writer.add(prepare(entry));
  }

  /**
   * Add an added entry for a file.
   *
   * <p>The entry's snapshot ID will be this manifest's snapshot ID. The data and file sequence
   * numbers will be assigned at commit.
   *
   * @param addedFile a data file
   */
  @Override
  public void add(F addedFile) {
    addEntry(reused.wrapAppend(snapshotId, addedFile));
  }

  /**
   * Add an added entry for a file with a specific sequence number.
   *
   * <p>The entry's snapshot ID will be this manifest's snapshot ID. The entry's data sequence
   * number will be the provided data sequence number. The entry's file sequence number will be
   * assigned at commit.
   *
   * @param addedFile a data file
   * @param dataSequenceNumber a data sequence number for the file
   */
  public void add(F addedFile, long dataSequenceNumber) {
    addEntry(reused.wrapAppend(snapshotId, dataSequenceNumber, addedFile));
  }

  void add(ManifestEntry<F> entry) {
    if (entry.dataSequenceNumber() != null && entry.dataSequenceNumber() >= 0) {
      addEntry(reused.wrapAppend(snapshotId, entry.dataSequenceNumber(), entry.file()));
    } else {
      addEntry(reused.wrapAppend(snapshotId, entry.file()));
    }
  }

  /**
   * Add an existing entry for a file.
   *
   * <p>The original data and file sequence numbers, snapshot ID, which were assigned at commit,
   * must be preserved when adding an existing entry.
   *
   * @param existingFile a file
   * @param fileSnapshotId snapshot ID when the data file was added to the table
   * @param dataSequenceNumber a data sequence number of the file (assigned when the file was added)
   * @param fileSequenceNumber a file sequence number (assigned when the file was added)
   */
  public void existing(
      F existingFile, long fileSnapshotId, long dataSequenceNumber, Long fileSequenceNumber) {
    reused.wrapExisting(fileSnapshotId, dataSequenceNumber, fileSequenceNumber, existingFile);
    addEntry(reused);
  }

  void existing(ManifestEntry<F> entry) {
    addEntry(reused.wrapExisting(entry));
  }

  /**
   * Add a delete entry for a file.
   *
   * <p>The entry's snapshot ID will be this manifest's snapshot ID. However, the original data and
   * file sequence numbers of the file must be preserved when the file is marked as deleted.
   *
   * @param deletedFile a file
   * @param dataSequenceNumber a data sequence number of the file (assigned when the file was added)
   * @param fileSequenceNumber a file sequence number (assigned when the file was added)
   */
  public void delete(F deletedFile, long dataSequenceNumber, Long fileSequenceNumber) {
    addEntry(reused.wrapDelete(snapshotId, dataSequenceNumber, fileSequenceNumber, deletedFile));
  }

  void delete(ManifestEntry<F> entry) {
    // Use the current Snapshot ID for the delete. It is safe to delete the data file from disk
    // when this Snapshot has been removed or when there are no Snapshots older than this one.
    addEntry(reused.wrapDelete(snapshotId, entry));
  }

  @Override
  public Metrics metrics() {
    return writer.metrics();
  }

  @Override
  public long length() {
    return writer.length();
  }

  public ManifestFile toManifestFile() {
    Preconditions.checkState(closed, "Cannot build ManifestFile, writer is not closed");

    ByteBuffer keyMetadataBuffer = keyMetadataBuffer();

    // if the minSequenceNumber is null, then no manifests with a sequence number have been written,
    // so the min data sequence number is the one that will be assigned when this is committed.
    // pass UNASSIGNED_SEQ to inherit it.
    long minSeqNumber = minDataSequenceNumber != null ? minDataSequenceNumber : UNASSIGNED_SEQ;
    return new GenericManifestFile(
        file.location(),
        writer.length(),
        specId,
        content(),
        UNASSIGNED_SEQ,
        minSeqNumber,
        snapshotId,
        stats.summaries(),
        keyMetadataBuffer,
        addedFiles,
        addedRows,
        existingFiles,
        existingRows,
        deletedFiles,
        deletedRows,
        firstRowId);
  }

  private ByteBuffer keyMetadataBuffer() {
    if (keyMetadata instanceof NativeEncryptionKeyMetadata nativeKeyMetadata
        && format == FileFormat.AVRO) {
      // Whole-file encryption needs the file length embedded for GCM truncation protection.
      // Formats with native encryption (like Parquet) handle this directly and don't need it.
      return nativeKeyMetadata.copyWithLength(length()).buffer();
    } else if (keyMetadata != null) {
      return keyMetadata.buffer();
    }

    return null;
  }

  @Override
  public void close() throws IOException {
    this.closed = true;
    writer.close();
  }

  /**
   * A {@link ManifestEntry} wrapper that delegates {@link StructLike} access to a {@code
   * TrackedFile} struct for writing content_entry rows in v4 leaf manifests.
   */
  private static class ContentEntryWriterEntry<F extends ContentFile<F>>
      implements ManifestEntry<F>, StructLike {
    private ManifestEntry<F> wrapped;
    private StructLike trackedStruct;

    ContentEntryWriterEntry<F> wrap(ManifestEntry<F> entry, TrackedFile trackedFile) {
      this.wrapped = entry;
      // TrackedFile is package-private and has a single impl (TrackedFileStruct) that implements
      // StructLike via SupportsIndexProjection. Keeping StructLike off the TrackedFile interface
      // matches the convention in V1/V2/V3 metadata (ContentFile, ManifestEntry, ManifestFile are
      // all kept clean of StructLike — their internal wrappers add it).
      this.trackedStruct = (StructLike) trackedFile;
      return this;
    }

    @Override
    public int size() {
      return trackedStruct.size();
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return trackedStruct.get(pos, javaClass);
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("ContentEntryWriterEntry is read-only");
    }

    @Override
    public Status status() {
      return wrapped.status();
    }

    @Override
    public Long snapshotId() {
      return wrapped.snapshotId();
    }

    @Override
    public Long dataSequenceNumber() {
      return wrapped.dataSequenceNumber();
    }

    @Override
    public Long fileSequenceNumber() {
      return wrapped.fileSequenceNumber();
    }

    @Override
    public F file() {
      return wrapped.file();
    }

    @Override
    public ManifestEntry<F> copy() {
      return wrapped.copy();
    }

    @Override
    public ManifestEntry<F> copyWithoutStats() {
      return wrapped.copyWithoutStats();
    }

    @Override
    public void setSnapshotId(long snapshotId) {
      wrapped.setSnapshotId(snapshotId);
    }

    @Override
    public void setDataSequenceNumber(long dataSequenceNumber) {
      wrapped.setDataSequenceNumber(dataSequenceNumber);
    }

    @Override
    public void setFileSequenceNumber(long fileSequenceNumber) {
      wrapped.setFileSequenceNumber(fileSequenceNumber);
    }
  }

  static class V4Writer extends ManifestWriter<DataFile> {
    /**
     * Placeholder partition struct used when the union partition type is empty (i.e., every live
     * spec in the table is unpartitioned). Parquet cannot encode an empty {@code
     * Types.StructType.of()} as a physical column, so a single dummy optional boolean field is used
     * instead. This field is always written as null and is ignored on read. Mirrors the placeholder
     * used by {@code RootManifestWriter} so the on-disk schema shape stays consistent across the v4
     * metadata tree.
     */
    static final Types.StructType EMPTY_PARTITION_PLACEHOLDER =
        Types.StructType.of(
            Types.NestedField.optional(99999, "_unpartitioned", Types.BooleanType.get()));

    /** Replaces an empty partition type with the placeholder; otherwise returns the input. */
    static Types.StructType emptyPartitionPlaceholderIfNeeded(Types.StructType partitionType) {
      return partitionType.fields().isEmpty() ? EMPTY_PARTITION_PLACEHOLDER : partitionType;
    }

    // ManifestWriter's super-constructor calls newAppender() before V4Writer's constructor body
    // sets fields, so unionPartitionType must be available to newAppender via a side channel. A
    // ThreadLocal is the simplest unobtrusive option that does not require restructuring the
    // ManifestWriter hierarchy. Each V4Writer constructor stashes the union type before super(...)
    // and clears it on its first use inside newAppender().
    private static final ThreadLocal<Types.StructType> PENDING_UNION_PARTITION_TYPE =
        new ThreadLocal<>();

    private final Schema tableSchema;
    private final Types.StructType unionPartitionType;
    private final ContentEntryWriterEntry<DataFile> writerEntry;

    V4Writer(
        PartitionSpec spec,
        Types.StructType unionPartitionType,
        EncryptedOutputFile file,
        Long snapshotId,
        Long firstRowId,
        Map<String, String> writerProperties) {
      super(
          stashUnionPartitionType(spec, unionPartitionType),
          file,
          snapshotId,
          firstRowId,
          writerProperties);
      this.tableSchema = spec.schema();
      this.unionPartitionType = unionPartitionType;
      this.writerEntry = new ContentEntryWriterEntry<>();
    }

    private static PartitionSpec stashUnionPartitionType(
        PartitionSpec spec, Types.StructType unionPartitionType) {
      PENDING_UNION_PARTITION_TYPE.set(unionPartitionType);
      return spec;
    }

    @Override
    protected ManifestEntry<DataFile> prepare(ManifestEntry<DataFile> entry) {
      TrackedFile trackedFile =
          ContentEntryAdapters.fromDataFile(
              entry, tableSchema, unionPartitionType, toEntryStatus(entry.status()));
      return writerEntry.wrap(entry, trackedFile);
    }

    @Override
    public void add(DataFile addedFile) {
      // v4 stores firstRowId per-entry in the tracking struct; do not suppress it.
      addEntry(reusedEntry().wrapAppendPreservingFirstRowId(writerSnapshotId(), null, addedFile));
    }

    @Override
    public ManifestFile toManifestFile() {
      // Set the on-disk record_count for the leaf manifest reference so the v4 root manifest's
      // content_entry row at field id 103 carries the actual entry count (including any statuses
      // not surfaced through the per-status accessors on ManifestFile).
      GenericManifestFile result = (GenericManifestFile) super.toManifestFile();
      result.setRecordCount(entriesWritten());
      return result;
    }

    @Override
    protected FileAppender<ManifestEntry<DataFile>> newAppender(
        PartitionSpec spec, OutputFile file) {
      Types.StructType partitionType = PENDING_UNION_PARTITION_TYPE.get();
      PENDING_UNION_PARTITION_TYPE.remove();
      Preconditions.checkArgument(
          partitionType != null, "Invalid union partition type: null (writer mis-initialized)");
      Schema contentEntrySchema =
          new Schema(
              TrackedFile.schemaWithContentStats(
                      emptyPartitionPlaceholderIfNeeded(partitionType),
                      StatsUtil.contentStatsFor(spec.schema()).type().asStructType())
                  .fields());
      try {
        return InternalData.write(format(), file)
            .schema(contentEntrySchema)
            .named("content_entry")
            .meta("schema", SchemaParser.toJson(spec.schema()))
            .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
            .meta("partition-spec-id", String.valueOf(spec.specId()))
            .meta("format-version", "4")
            .meta("content", "data")
            .set(writerProperties())
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to create manifest writer for path: %s", file.location());
      }
    }
  }

  static class V4DeleteWriter extends ManifestWriter<DeleteFile> {
    // See V4Writer.PENDING_UNION_PARTITION_TYPE for why a ThreadLocal is necessary.
    private static final ThreadLocal<Types.StructType> PENDING_UNION_PARTITION_TYPE =
        new ThreadLocal<>();

    private final Schema tableSchema;
    private final Types.StructType unionPartitionType;
    private final ContentEntryWriterEntry<DeleteFile> writerEntry;

    V4DeleteWriter(
        PartitionSpec spec,
        Types.StructType unionPartitionType,
        EncryptedOutputFile file,
        Long snapshotId,
        Map<String, String> writerProperties) {
      super(
          stashUnionPartitionType(spec, unionPartitionType),
          file,
          snapshotId,
          null,
          writerProperties);
      this.tableSchema = spec.schema();
      this.unionPartitionType = unionPartitionType;
      this.writerEntry = new ContentEntryWriterEntry<>();
    }

    private static PartitionSpec stashUnionPartitionType(
        PartitionSpec spec, Types.StructType unionPartitionType) {
      PENDING_UNION_PARTITION_TYPE.set(unionPartitionType);
      return spec;
    }

    @Override
    protected ManifestEntry<DeleteFile> prepare(ManifestEntry<DeleteFile> entry) {
      TrackedFile trackedFile =
          ContentEntryAdapters.fromDeleteFile(
              entry, tableSchema, unionPartitionType, toEntryStatus(entry.status()));
      return writerEntry.wrap(entry, trackedFile);
    }

    @Override
    public ManifestFile toManifestFile() {
      // See V4Writer.toManifestFile for why record_count is set explicitly on v4 leaf writers.
      GenericManifestFile result = (GenericManifestFile) super.toManifestFile();
      result.setRecordCount(entriesWritten());
      return result;
    }

    @Override
    protected FileAppender<ManifestEntry<DeleteFile>> newAppender(
        PartitionSpec spec, OutputFile file) {
      Types.StructType partitionType = PENDING_UNION_PARTITION_TYPE.get();
      PENDING_UNION_PARTITION_TYPE.remove();
      Preconditions.checkArgument(
          partitionType != null,
          "Invalid union partition type: null (delete writer mis-initialized)");
      Schema contentEntrySchema =
          new Schema(
              TrackedFile.schemaWithContentStats(
                      V4Writer.emptyPartitionPlaceholderIfNeeded(partitionType),
                      StatsUtil.contentStatsFor(spec.schema()).type().asStructType())
                  .fields());
      try {
        return InternalData.write(format(), file)
            .schema(contentEntrySchema)
            .named("content_entry")
            .meta("schema", SchemaParser.toJson(spec.schema()))
            .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
            .meta("partition-spec-id", String.valueOf(spec.specId()))
            .meta("format-version", "4")
            .meta("content", "deletes")
            .set(writerProperties())
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to create manifest writer for path: %s", file.location());
      }
    }

    @Override
    protected ManifestContent content() {
      return ManifestContent.DELETES;
    }
  }

  private static EntryStatus toEntryStatus(ManifestEntry.Status status) {
    switch (status) {
      case EXISTING:
        return EntryStatus.EXISTING;
      case ADDED:
        return EntryStatus.ADDED;
      case DELETED:
        return EntryStatus.DELETED;
      default:
        throw new IllegalArgumentException("Unknown manifest entry status: " + status);
    }
  }

  static class V3Writer extends ManifestWriter<DataFile> {
    private final V3Metadata.ManifestEntryWrapper<DataFile> entryWrapper;

    V3Writer(
        PartitionSpec spec,
        EncryptedOutputFile file,
        Long snapshotId,
        Long firstRowId,
        Map<String, String> writerProperties) {
      super(spec, file, snapshotId, firstRowId, writerProperties);
      Preconditions.checkArgument(
          format() == FileFormat.AVRO, "V3 manifests must use Avro, but got: %s", format());
      this.entryWrapper = new V3Metadata.ManifestEntryWrapper<>(snapshotId);
    }

    @Override
    protected ManifestEntry<DataFile> prepare(ManifestEntry<DataFile> entry) {
      return entryWrapper.wrap(entry);
    }

    @Override
    protected FileAppender<ManifestEntry<DataFile>> newAppender(
        PartitionSpec spec, OutputFile file) {
      Schema manifestSchema = V3Metadata.entrySchema(spec.partitionType());
      try {
        return InternalData.write(FileFormat.AVRO, file)
            .schema(manifestSchema)
            .named("manifest_entry")
            .meta("schema", SchemaParser.toJson(spec.schema()))
            .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
            .meta("partition-spec-id", String.valueOf(spec.specId()))
            .meta("format-version", "3")
            .meta("content", "data")
            .set(writerProperties())
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to create manifest writer for path: %s", file.location());
      }
    }
  }

  static class V3DeleteWriter extends ManifestWriter<DeleteFile> {
    private final V3Metadata.ManifestEntryWrapper<DeleteFile> entryWrapper;

    V3DeleteWriter(
        PartitionSpec spec,
        EncryptedOutputFile file,
        Long snapshotId,
        Map<String, String> writerProperties) {
      super(spec, file, snapshotId, null, writerProperties);
      Preconditions.checkArgument(
          format() == FileFormat.AVRO, "V3 manifests must use Avro, but got: %s", format());
      this.entryWrapper = new V3Metadata.ManifestEntryWrapper<>(snapshotId);
    }

    @Override
    protected ManifestEntry<DeleteFile> prepare(ManifestEntry<DeleteFile> entry) {
      return entryWrapper.wrap(entry);
    }

    @Override
    protected FileAppender<ManifestEntry<DeleteFile>> newAppender(
        PartitionSpec spec, OutputFile file) {
      Schema manifestSchema = V3Metadata.entrySchema(spec.partitionType());
      try {
        return InternalData.write(FileFormat.AVRO, file)
            .schema(manifestSchema)
            .named("manifest_entry")
            .meta("schema", SchemaParser.toJson(spec.schema()))
            .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
            .meta("partition-spec-id", String.valueOf(spec.specId()))
            .meta("format-version", "3")
            .meta("content", "deletes")
            .set(writerProperties())
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to create manifest writer for path: %s", file.location());
      }
    }

    @Override
    protected ManifestContent content() {
      return ManifestContent.DELETES;
    }
  }

  static class V2Writer extends ManifestWriter<DataFile> {
    private final V2Metadata.ManifestEntryWrapper<DataFile> entryWrapper;

    V2Writer(
        PartitionSpec spec,
        EncryptedOutputFile file,
        Long snapshotId,
        Map<String, String> writerProperties) {
      super(spec, file, snapshotId, null, writerProperties);
      Preconditions.checkArgument(
          format() == FileFormat.AVRO, "V2 manifests must use Avro, but got: %s", format());
      this.entryWrapper = new V2Metadata.ManifestEntryWrapper<>(snapshotId);
    }

    @Override
    protected ManifestEntry<DataFile> prepare(ManifestEntry<DataFile> entry) {
      return entryWrapper.wrap(entry);
    }

    @Override
    protected FileAppender<ManifestEntry<DataFile>> newAppender(
        PartitionSpec spec, OutputFile file) {
      Schema manifestSchema = V2Metadata.entrySchema(spec.partitionType());
      try {
        return InternalData.write(FileFormat.AVRO, file)
            .schema(manifestSchema)
            .named("manifest_entry")
            .meta("schema", SchemaParser.toJson(spec.schema()))
            .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
            .meta("partition-spec-id", String.valueOf(spec.specId()))
            .meta("format-version", "2")
            .meta("content", "data")
            .set(writerProperties())
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to create manifest writer for path: %s", file.location());
      }
    }
  }

  static class V2DeleteWriter extends ManifestWriter<DeleteFile> {
    private final V2Metadata.ManifestEntryWrapper<DeleteFile> entryWrapper;

    V2DeleteWriter(
        PartitionSpec spec,
        EncryptedOutputFile file,
        Long snapshotId,
        Map<String, String> writerProperties) {
      super(spec, file, snapshotId, null, writerProperties);
      Preconditions.checkArgument(
          format() == FileFormat.AVRO, "V2 manifests must use Avro, but got: %s", format());
      this.entryWrapper = new V2Metadata.ManifestEntryWrapper<>(snapshotId);
    }

    @Override
    protected ManifestEntry<DeleteFile> prepare(ManifestEntry<DeleteFile> entry) {
      return entryWrapper.wrap(entry);
    }

    @Override
    protected FileAppender<ManifestEntry<DeleteFile>> newAppender(
        PartitionSpec spec, OutputFile file) {
      Schema manifestSchema = V2Metadata.entrySchema(spec.partitionType());
      try {
        return InternalData.write(FileFormat.AVRO, file)
            .schema(manifestSchema)
            .named("manifest_entry")
            .meta("schema", SchemaParser.toJson(spec.schema()))
            .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
            .meta("partition-spec-id", String.valueOf(spec.specId()))
            .meta("format-version", "2")
            .meta("content", "deletes")
            .set(writerProperties())
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to create manifest writer for path: %s", file.location());
      }
    }

    @Override
    protected ManifestContent content() {
      return ManifestContent.DELETES;
    }
  }

  static class V1Writer extends ManifestWriter<DataFile> {
    private final V1Metadata.ManifestEntryWrapper entryWrapper;

    V1Writer(
        PartitionSpec spec,
        EncryptedOutputFile file,
        Long snapshotId,
        Map<String, String> writerProperties) {
      super(spec, file, snapshotId, null, writerProperties);
      Preconditions.checkArgument(
          format() == FileFormat.AVRO, "V1 manifests must use Avro, but got: %s", format());
      this.entryWrapper = new V1Metadata.ManifestEntryWrapper();
    }

    @Override
    protected ManifestEntry<DataFile> prepare(ManifestEntry<DataFile> entry) {
      return entryWrapper.wrap(entry);
    }

    @Override
    protected FileAppender<ManifestEntry<DataFile>> newAppender(
        PartitionSpec spec, OutputFile file) {
      Schema manifestSchema = V1Metadata.entrySchema(spec.partitionType());
      try {
        return InternalData.write(FileFormat.AVRO, file)
            .schema(manifestSchema)
            .named("manifest_entry")
            .meta("schema", SchemaParser.toJson(spec.schema()))
            .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
            .meta("partition-spec-id", String.valueOf(spec.specId()))
            .meta("format-version", "1")
            .set(writerProperties())
            .overwrite()
            .build();
      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to create manifest writer for path: %s", file.location());
      }
    }
  }
}
