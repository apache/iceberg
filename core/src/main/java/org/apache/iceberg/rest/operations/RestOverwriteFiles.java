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
package org.apache.iceberg.rest.operations;

import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.MANIFEST_TARGET_SIZE_BYTES_DEFAULT;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RollingManifestWriter;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;

public class RestOverwriteFiles implements OverwriteFiles {
  private final RESTClient client;
  private final String path;
  private final Supplier<Map<String, String>> headers;
  private final TableOperations ops;
  private final PartitionSpec spec;
  private final long targetManifestSizeBytes;
  private final String commitUUID = UUID.randomUUID().toString();
  private final AtomicInteger manifestCount = new AtomicInteger(0);
  private volatile Long snapshotId = null;
  private Expression overwriteByRowFilterExpression;
  private Expression conflictDetectionFilter;
  private final List<DataFile> deletedDataFiles = Lists.newArrayList();
  private final List<DataFile> newDataFiles = Lists.newArrayList();
  private boolean caseSensitive = true;

  public RestOverwriteFiles(
      RESTClient client, String path, Supplier<Map<String, String>> headers, TableOperations ops) {
    this.client = client;
    this.path = path;
    this.headers = headers;
    this.ops = ops;
    this.spec = ops.current().spec();
    this.targetManifestSizeBytes =
        ops.current()
            .propertyAsLong(MANIFEST_TARGET_SIZE_BYTES, MANIFEST_TARGET_SIZE_BYTES_DEFAULT);
  }

  @Override
  public void commit() {
    MetadataUpdate.OverwriteFilesUpdate overwriteFilesUpdate = constructMetadataUpdate();
    List<UpdateRequirement> requirements =
        UpdateRequirements.forUpdateTable(ops.current(), ImmutableList.of(overwriteFilesUpdate));
    UpdateTableRequest request =
        new UpdateTableRequest(requirements, ImmutableList.of(overwriteFilesUpdate));
    client.post(
        path, request, LoadTableResponse.class, headers, ErrorHandlers.tableCommitHandler());
  }

  private MetadataUpdate.OverwriteFilesUpdate constructMetadataUpdate() {
    MetadataUpdate.OverwriteFilesUpdate update = new MetadataUpdate.OverwriteFilesUpdate();
    update.setAddedManifests(constructManifests(newDataFiles));
    update.setDeletedManifests(constructManifests(deletedDataFiles));
    update.setOverwriteByRowFilterExpression(overwriteByRowFilterExpression);
    update.setConflictExpression(conflictDetectionFilter);
    update.setCaseSensitive(caseSensitive);
    return update;
  }

  private List<String> constructManifests(List<DataFile> files) {
    List<ManifestFile> manifests = Lists.newArrayList();
    try {
      RollingManifestWriter<DataFile> writer = newRollingManifestWriter();
      try {
        files.forEach(writer::add);
      } finally {
        writer.close();
      }
      manifests.addAll(writer.toManifestFiles());
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write manifest");
    }
    return manifests.stream().map(ManifestFile::path).collect(Collectors.toList());
  }

  protected RollingManifestWriter<DataFile> newRollingManifestWriter() {
    return new RollingManifestWriter<>(this::newManifestWriter, targetManifestSizeBytes);
  }

  protected ManifestWriter<DataFile> newManifestWriter() {
    return ManifestFiles.write(
        ops.current().formatVersion(), spec, newManifestOutput(), snapshotId());
  }

  protected OutputFile newManifestOutput() {
    return ops.io()
        .newOutputFile(
            ops.metadataFileLocation(
                FileFormat.AVRO.addExtension(commitUUID + "-m" + manifestCount.getAndIncrement())));
  }

  protected long snapshotId() {
    if (snapshotId == null) {
      synchronized (this) {
        while (snapshotId == null || ops.current().snapshot(snapshotId) != null) {
          this.snapshotId = ops.newSnapshotId();
        }
      }
    }
    return snapshotId;
  }

  @Override
  public OverwriteFiles overwriteByRowFilter(Expression expression) {
    this.overwriteByRowFilterExpression = expression;
    return this;
  }

  @Override
  public OverwriteFiles addFile(DataFile file) {
    newDataFiles.add(file);
    return this;
  }

  @Override
  public OverwriteFiles deleteFile(DataFile file) {
    deletedDataFiles.add(file);
    return this;
  }

  @Override
  public OverwriteFiles validateAddedFilesMatchOverwriteFilter() {
    return this;
  }

  @Override
  public OverwriteFiles validateFromSnapshot(long newSnapshotId) {
    return this;
  }

  @Override
  public OverwriteFiles caseSensitive(boolean isCaseSensitive) {
    this.caseSensitive = isCaseSensitive;
    return this;
  }

  @Override
  public OverwriteFiles conflictDetectionFilter(Expression expression) {
    this.conflictDetectionFilter = expression;
    return this;
  }

  @Override
  public OverwriteFiles validateNoConflictingData() {
    return this;
  }

  @Override
  public OverwriteFiles validateNoConflictingDeletes() {
    return this;
  }

  @Override
  public OverwriteFiles set(String property, String value) {
    return this;
  }

  @Override
  public OverwriteFiles deleteWith(Consumer<String> deleteFunc) {
    return this;
  }

  @Override
  public OverwriteFiles stageOnly() {
    return this;
  }

  @Override
  public OverwriteFiles scanManifestsWith(ExecutorService executorService) {
    return this;
  }

  @Override
  public Snapshot apply() {
    return null;
  }
}
