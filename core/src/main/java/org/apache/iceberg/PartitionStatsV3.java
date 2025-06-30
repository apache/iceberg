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

public class PartitionStatsV3 extends PartitionStats {

  private int dvCount;

  public PartitionStatsV3(StructLike partition, int specId) {
    super(partition, specId);
  }

  public int dvCount() {
    return dvCount;
  }

  @Override
  public void liveEntry(ContentFile<?> file, Snapshot snapshot) {
    super.liveEntry(file, snapshot);
    if (file.content() == FileContent.POSITION_DELETES && file.format() == FileFormat.PUFFIN) {
      this.dvCount += 1;
      // revert the changes for position delete file count increment from the parent method
      this.set(6, positionDeleteFileCount() - 1);
    }
  }

  @Override
  void deletedEntryForIncrementalCompute(ContentFile<?> file, Snapshot snapshot) {
    super.deletedEntryForIncrementalCompute(file, snapshot);
    if (file.content() == FileContent.POSITION_DELETES && file.format() == FileFormat.PUFFIN) {
      this.dvCount -= 1;
      // revert the changes for position delete file count decrement from the parent method
      this.set(6, positionDeleteFileCount() + 1);
    }
  }

  @Override
  @Deprecated // will become package-private
  public void appendStats(PartitionStats entry) {
    super.appendStats(entry);

    if (entry instanceof PartitionStatsV3) {
      this.dvCount += ((PartitionStatsV3) entry).dvCount;
    }
  }

  @Override
  public int size() {
    // includes dv counter
    return STATS_COUNT + 1;
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    if (pos == STATS_COUNT) {
      return javaClass.cast(dvCount);
    } else {
      return super.get(pos, javaClass);
    }
  }

  @Override
  public <T> void set(int pos, T value) {
    if (pos == STATS_COUNT) {
      this.dvCount = value == null ? 0 : (int) value;
    } else {
      super.set(pos, value);
    }
  }
}
