/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.netflix.iceberg.expressions.StrictMetricsEvaluator;
import com.netflix.iceberg.util.CharSequenceWrapper;
import com.netflix.iceberg.exceptions.CommitFailedException;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.exceptions.ValidationException;
import com.netflix.iceberg.expressions.Evaluator;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.Expressions;
import com.netflix.iceberg.expressions.Projections;
import com.netflix.iceberg.io.OutputFile;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link DeleteFiles Delete} implementation that avoids loading full manifests in memory.
 * <p>
 * This implementation will attempt to commit 5 times before throwing {@link CommitFailedException}.
 */
class StreamingDelete extends SnapshotUpdate implements DeleteFiles {
  private final TableOperations ops;
  private final StrictMetricsEvaluator metricsEvaluator;
  private final Set<CharSequenceWrapper> deletePaths = Sets.newHashSet();
  private Expression deleteExpression = Expressions.alwaysFalse();

  // cache filtered manifests to avoid extra work when commits fail.
  private final Map<String, String> filteredManifests = Maps.newHashMap();
  private boolean filterUpdated = false; // used to clear filteredManifests

  StreamingDelete(TableOperations ops) {
    super(ops);
    this.ops = ops;

    // use a common metrics evaluator for all manifests because it is bound to the table schema
    // do not change the schema even if the table is updated because the intent was to use the
    // schema when the delete was started
    this.metricsEvaluator = new StrictMetricsEvaluator(ops.current().schema(), deleteExpression);
  }

  @Override
  public StreamingDelete deleteFile(CharSequence path) {
    Preconditions.checkNotNull(path, "Cannot delete file path: null");
    this.filterUpdated = true;
    deletePaths.add(CharSequenceWrapper.wrap(path));
    return this;
  }

  @Override
  public StreamingDelete deleteFromRowFilter(Expression expr) {
    this.filterUpdated = true;
    this.deleteExpression = Expressions.or(deleteExpression, expr);
    return this;
  }

  @Override
  public List<String> apply(TableMetadata base) {
    // if the filter has been updated since the last apply, clean up the cache
    if (filterUpdated) {
      cleanAll();
      this.filterUpdated = false;
    }

    List<String> newManifests = Lists.newArrayList();
    for (String manifest : base.currentSnapshot().manifests()) {
      String newManifest = filterManifest(manifest);
      if (newManifest != null) {
        newManifests.add(newManifest);
      }
    }

    return newManifests;
  }

  @Override
  protected void cleanUncommitted(Set<String> committed) {
    for (Map.Entry<String, String> entry: filteredManifests.entrySet()) {
      // remove any new filtered manifests that aren't in the committed list
      String manifest = entry.getKey();
      String filtered = entry.getValue();
      if (filtered != null && !manifest.equals(filtered) && !committed.contains(filtered)) {
        deleteFile(filtered);
      }
    }
    filteredManifests.clear();
  }

  private String filterManifest(String manifest) {
    if (filteredManifests.containsKey(manifest)) {
      return filteredManifests.get(manifest);
    }

    OutputFile filteredCopy = manifestPath(filteredManifests.size());

    try (ManifestReader reader = ManifestReader.read(ops.newInputFile(manifest))) {
      Expression inclusiveExpr = Projections
          .inclusive(reader.spec())
          .project(deleteExpression);
      Evaluator inclusive = new Evaluator(reader.spec().partitionType(), inclusiveExpr);

      Expression strictExpr = Projections
          .strict(reader.spec())
          .project(deleteExpression);
      Evaluator strict = new Evaluator(reader.spec().partitionType(), strictExpr);

      // this is reused to compare file paths with the delete set
      CharSequenceWrapper wrapper = CharSequenceWrapper.wrap("");

      long deletedFilesCount = 0;
      try (ManifestWriter writer = new ManifestWriter(reader.spec(), filteredCopy, snapshotId())) {
        for (ManifestEntry entry : reader.entries()) {
          DataFile file = entry.file();
          boolean fileDelete = deletePaths.contains(wrapper.set(file.path()));
          if (fileDelete || inclusive.eval(file.partition())) {
            ValidationException.check(
                fileDelete || strict.eval(file.partition()) || metricsEvaluator.eval(file),
                "Cannot delete file where some, but not all, rows match filter %s: %s",
                deleteExpression, file.path());

            deletedFilesCount += 1;
            writer.delete(entry);

          } else {
            writer.addExisting(entry);
          }
        }
      }

      // only use the new manifest if this produced changes.
      String filtered;
      if (deletedFilesCount > 0) {
        filtered = filteredCopy.location();
      } else {
        deleteFile(filteredCopy.location());
        filtered = reader.file().location();
      }

      filteredManifests.put(manifest, filtered);

      return filtered;

    } catch (IOException e) {
      throw new RuntimeIOException(e,
          "Failed to rewrite manifest: %s -> %s", manifest, filteredCopy);
    }
  }
}
