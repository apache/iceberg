package org.apache.iceberg.delta;

import org.apache.iceberg.*;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class DeltaManifestWriter implements FileAppender<DeltaFile> {
  private static final Logger LOG = LoggerFactory.getLogger(ManifestWriter.class);



 public static DeltaManifestWriter write(TableMetadata metadata, DeltaSnapshot snapshot, OutputFile file) {
    return new DeltaManifestWriter(metadata.spec(), metadata.pkSpec(), file, snapshot.snapshotId(),
            snapshot.parentId(), snapshot.parentManifestLocation());
  }

  private final OutputFile file;
  private final int specId;
  private final FileAppender<GenericDeltaManifestEntry> writer;
  private final Long snapshotId;
  private final Long parentSnapshotId;
  private final String parentDeltaManifestPath;
  private final GenericDeltaManifestEntry reused;

  private boolean closed = false;
  private long rowCount = 0L;
  private long deletedRows = 0L;
  private int fileCount = 0;

  DeltaManifestWriter(PartitionSpec spec, PrimaryKeySpec pkSpec, OutputFile file, Long snapshotId,
                      Long parentSnapshotId, String parentDeltaManifestPath) {
    this.file = file;
    this.specId = spec.specId();
    this.writer = newAppender(FileFormat.AVRO, spec, pkSpec, file,
        parentSnapshotId, parentDeltaManifestPath);
    this.snapshotId = snapshotId;
    this.reused = new GenericDeltaManifestEntry(spec.partitionType(), pkSpec.primaryKeyType());
    this.parentSnapshotId = parentSnapshotId;
    this.parentDeltaManifestPath = parentDeltaManifestPath;
  }

  void addEntry(GenericDeltaManifestEntry entry) {
    fileCount += 1;
    rowCount += entry.file().rowCount();
    deletedRows += entry.file().deleteCount();
    writer.add(entry);
  }

  @Override
  public void add(DeltaFile addedFile) {
    addEntry(reused.wrap(snapshotId, addedFile));
  }

  public void add(GenericDeltaManifestEntry entry) {
    addEntry(reused.wrap(snapshotId, entry.file()));
  }


  @Override
  public Metrics metrics() {
    return writer.metrics();
  }

  @Override
  public long length() {
    return writer.length();
  }

  @Override
  public void close() throws IOException {
    this.closed = true;
    writer.close();
  }

  private static <D> FileAppender<D> newAppender(FileFormat format, PartitionSpec spec, PrimaryKeySpec pkSpec,
                                                 OutputFile file, Long parentSnapshotId,
                                                 String parentDeltaManifestPath) {
    Schema manifestSchema = DeltaManifestEntry.getSchema(spec.partitionType(), pkSpec.primaryKeyType());
    try {
      switch (format) {
        case AVRO:
          return Avro.write(file)
              .schema(manifestSchema)
              .named("delta_manifest_entry")
              .meta("schema", SchemaParser.toJson(spec.schema()))
              .meta("partition-spec", PartitionSpecParser.toJsonFields(spec))
              .meta("partition-spec-id", String.valueOf(spec.specId()))
              .meta("parent-snapshot-id", String.valueOf(parentSnapshotId))
              .meta("parent-delta-manifest", String.valueOf(parentDeltaManifestPath))
              .overwrite()
              .build();
        default:
          throw new IllegalArgumentException("Unsupported format: " + format);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to create manifest writer for path: " + file);
    }
  }
}
