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


  /**
   * Create a new {@link ManifestWriter}.
   * <p>
   * Manifests created by this writer have all entry snapshot IDs set to null.
   * All entries will inherit the snapshot ID that will be assigned to the manifest on commit.
   *
   * @param spec {@link PartitionSpec} used to produce {@link DataFile} partition tuples
   * @param outputFile the destination file location
   * @return a manifest writer
   */
 /* public static DeltaManifestWriter write(PartitionSpec spec, OutputFile outputFile,
                                          Long parentSnapshotId, String parentDeltaManifestPat) {
    return new DeltaManifestWriter(spec, outputFile, null);
  }*/

  private final OutputFile file;
  private final int specId;
  private final FileAppender<DeltaManifestEntry> writer;
  private final Long snapshotId;
  private final Long parentSnapshotId;
  private final String parentDeltaManifestPath;
  private final DeltaManifestEntry reused;

  private boolean closed = false;
  private long rowCount = 0L;
  private long deletedRows = 0L;
  private int fileCount = 0;

  DeltaManifestWriter(PartitionSpec spec, OutputFile file, Long snapshotId,
                      Long parentSnapshotId, String parentDeltaManifestPath) {
    this.file = file;
    this.specId = spec.specId();
    this.writer = newAppender(FileFormat.AVRO, spec, file,
        parentSnapshotId, parentDeltaManifestPath);
    this.snapshotId = snapshotId;
    this.reused = new DeltaManifestEntry(spec.partitionType());
    this.parentSnapshotId = parentSnapshotId;
    this.parentDeltaManifestPath = parentDeltaManifestPath;
  }

  void addEntry(DeltaManifestEntry entry) {
    fileCount += 1;
    rowCount += entry.file().rowCount();
    deletedRows += entry.file().deleteCount();
    writer.add(entry);
  }

  /**
   * Add an added entry for a data file.
   * <p>
   * The entry's snapshot ID will be this manifest's snapshot ID.
   *
   * @param addedFile a data file
   */
  @Override
  public void add(DeltaFile addedFile) {
    // TODO: this assumes that file is a GenericDataFile that can be written directly to Avro
    // Eventually, this should check in case there are other DataFile implementations.
    addEntry(reused.wrap(snapshotId, addedFile));
  }

  public void add(DeltaManifestEntry entry) {
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

/*  public ManifestFile toManifestFile() {
    Preconditions.checkState(closed, "Cannot build ManifestFile, writer is not closed");
    return new GenericManifestFile(file.location(), writer.length(), specId, snapshotId,
        addedFiles, addedRows, existingFiles, existingRows, deletedFiles, deletedRows, stats.summaries());
  }*/

  @Override
  public void close() throws IOException {
    this.closed = true;
    writer.close();
  }

  private static <D> FileAppender<D> newAppender(FileFormat format, PartitionSpec spec,
                                                 OutputFile file, Long parentSnapshotId,
                                                 String parentDeltaManifestPath) {
    Schema manifestSchema = DeltaManifestEntry.getSchema(spec.partitionType());
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
