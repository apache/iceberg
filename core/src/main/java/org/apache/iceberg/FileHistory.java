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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceWrapper;

public class FileHistory {
  private static final List<String> HISTORY_COLUMNS = ImmutableList.of("file_path");

  private FileHistory() {
  }

  public static Builder table(Table table) {
    return new Builder(table);
  }

  public static class Builder {
    private final Table table;
    private final Set<CharSequenceWrapper> locations = Sets.newHashSet();
    private Long startTime = null;
    private Long endTime = null;

    public Builder(Table table) {
      this.table = table;
    }

    public Builder location(String location) {
      locations.add(CharSequenceWrapper.wrap(location));
      return this;
    }

    public Builder after(String timestamp) {
      Literal<Long> tsLiteral = Literal.of(timestamp).to(Types.TimestampType.withoutZone());
      this.startTime = tsLiteral.value() / 1000;
      return this;
    }

    public Builder after(long timestampMillis) {
      this.startTime = timestampMillis;
      return this;
    }

    public Builder before(String timestamp) {
      Literal<Long> tsLiteral = Literal.of(timestamp).to(Types.TimestampType.withoutZone());
      this.endTime = tsLiteral.value() / 1000;
      return this;
    }

    public Builder before(long timestampMillis) {
      this.endTime = timestampMillis;
      return this;
    }

    @SuppressWarnings("unchecked")
    public Iterable<ManifestEntry> build() {
      Iterable<Snapshot> snapshots = table.snapshots();

      if (startTime != null) {
        snapshots = Iterables.filter(snapshots, snap -> snap.timestampMillis() >= startTime);
      }

      if (endTime != null) {
        snapshots = Iterables.filter(snapshots, snap -> snap.timestampMillis() <= endTime);
      }

      // only use manifests that were added in the matching snapshots
      Set<Long> matchingIds = Sets.newHashSet(Iterables.transform(snapshots, Snapshot::snapshotId));
      Iterable<ManifestFile> manifests = Iterables.filter(
          Iterables.concat(Iterables.transform(snapshots, Snapshot::manifests)),
          manifest -> manifest.snapshotId() == null || matchingIds.contains(manifest.snapshotId()));

      // a manifest group will only read each manifest once
      ManifestGroup group = new ManifestGroup(((HasTableOperations) table).operations(), manifests);

      List<ManifestEntry> results = Lists.newArrayList();
      try (CloseableIterable<ManifestEntry> entries = group.select(HISTORY_COLUMNS).entries()) {
        // TODO: replace this with an IN predicate
        CharSequenceWrapper locationWrapper = CharSequenceWrapper.wrap(null);
        for (ManifestEntry entry : entries) {
          if (entry != null && locations.contains(locationWrapper.set(entry.file().path()))) {
            results.add(entry.copy());
          }
        }
      } catch (IOException e) {
        throw new RuntimeIOException(e);
      }

      return results;
    }
  }
}
