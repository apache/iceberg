package com.netflix.iceberg;

import avro.shaded.com.google.common.collect.Iterables;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.exceptions.ValidationException;
import com.netflix.iceberg.io.OutputFile;
import com.netflix.iceberg.util.Tasks;
import com.netflix.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.netflix.iceberg.ManifestEntry.Status.DELETED;
import static java.util.Collections.synchronizedList;
import static java.util.Collections.synchronizedSet;

public abstract class BaseReplaceFiles extends SnapshotUpdate {

  private final Logger LOG = LoggerFactory.getLogger(getClass());

  private final TableOperations ops;
  private final List<String> newManifests = synchronizedList(new ArrayList<>());
  private List<String> manifestFiles = synchronizedList(new ArrayList<>());
  private Set<String> deletedFiles = synchronizedSet(new HashSet<>());
  private String appendManifest;
  private final AtomicInteger manifestCount = new AtomicInteger(0);

  protected Set<DataFile> filesToAdd = new HashSet<>();
  protected boolean hasNewFiles;

  public BaseReplaceFiles(TableOperations ops) {
    super(ops);
    this.ops = ops;
  }

  protected void addFiles(PartitionSpec spec) {
    if (this.hasNewFiles) {
      OutputFile out = manifestPath(manifestCount.getAndIncrement());
      try (ManifestWriter writer = new ManifestWriter(spec, out, snapshotId())) {
        writer.addAll(this.filesToAdd);
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to write manifest: %s", out);
      }
      appendManifest = out.location();
      newManifests.add(appendManifest);
      this.hasNewFiles = false;
    }
    manifestFiles.add(appendManifest);
  }


  protected List<String> apply(TableMetadata base) {
    final Snapshot snapshot = base.currentSnapshot();
    ValidationException.check(snapshot != null, "No snapshots are committed.");

    this.manifestFiles.clear();
    this.deletedFiles.clear();

    Tasks.foreach(snapshot.manifests())
      .noRetry()
      .stopOnFailure()
      .throwFailureWhenFinished()
      .executeWith(ThreadPools.getWorkerPool())
      .run(manifest -> {
        try (ManifestReader reader = ManifestReader.read(ops.newInputFile(manifest))) {
          final OutputFile manifestPath = manifestPath(manifestCount.getAndIncrement());
          try (ManifestWriter writer = new ManifestWriter(reader.spec(), manifestPath, snapshotId())) {
            boolean hasDeletes = false;
            final Iterable<ManifestEntry> notDeletedManifestEntries = notDeletedManifestEntries(reader);
            for (ManifestEntry manifestEntry : notDeletedManifestEntries) {
              if (shouldDelete(reader.spec()).test(manifestEntry)) {
                hasDeletes = true;
                writer.delete(manifestEntry);
                final String deletedPath = manifestEntry.file().path().toString();
                if(deletedFiles.contains(deletedPath)) {
                  LOG.warn(String.format("Deleting a duplicated path %s from manifest %s", deletedPath, manifest));
                }
                deletedFiles.add(deletedPath);
              } else {
                writer.addExisting(manifestEntry);
              }
            }

            if (hasDeletes) {
              this.manifestFiles.add(manifestPath.location());
            } else {
              this.manifestFiles.add(manifest);
            }
          } catch (IOException e) {
            throw new RuntimeIOException(e);
          } finally {
            newManifests.add(manifestPath.location());
          }
        } catch (IOException e) {
          throw new RuntimeIOException(e);
        }
      });

    addFiles(base.spec());
    return this.manifestFiles.stream().collect(Collectors.toList());
  }

  private Iterable<ManifestEntry> notDeletedManifestEntries(ManifestReader reader) {
    return Iterables.filter(reader.entries(), entry -> entry.status() != DELETED);
  }

  protected Set<String> deletedFiles() {
    return this.deletedFiles;
  }

  @Override
  protected void cleanUncommitted(Set<String> committed) {
    this.newManifests.stream().filter(m -> !committed.contains(m)).forEach(m -> deleteFile(m));
    this.newManifests.clear();
  }

  /**
   * Function that would be used during manifest processing to chose if a given manifest entry
   * should be deleted or not.
   * @param spec the partition spec of the manifest file that is being processed.
   * @return a predicate that can decide if a manifest entry should be deleted or not.
   */
  protected abstract Predicate<ManifestEntry> shouldDelete(PartitionSpec spec);
}
