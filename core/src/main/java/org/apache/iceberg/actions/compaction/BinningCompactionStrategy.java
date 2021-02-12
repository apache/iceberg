/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.actions.compaction;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.util.BinPacking;

import static org.apache.iceberg.util.StreamUtil.streamOf;

/**
 * A Compaction strategy for rewriting files based on their sizes, the goal is to end up with files
 * whose size is close to the Target Size as specified in this strategy's options.
 */
public abstract class BinningCompactionStrategy implements DataCompactionStrategy {

  // TODO Maybe these should be global?
  private static final String TARGET_SIZE_OPTION = "target_size";
  private static final String TARGET_THRESHOLD_OPTION = "target_threshold";
  private static final String TARGET_MAX_JOB_SIZE_OPTION = "max_job_size";

  protected static final long TARGET_THRESHOLD_DEFAULT = 50 * 1024 * 1024; // 50 MB
  protected static final long TARGET_SIZE_DEFAULT = 512 * 1024 * 1024; // 512 MB (Perhaps always have this passed through from table? target write size?)
  protected static final long MAX_JOB_SIZE_DEFAULT = 100L * 1024 * 1024 * 1024; // 100 GB

  protected long targetThreshold = TARGET_THRESHOLD_DEFAULT;
  protected long targetSize = TARGET_SIZE_DEFAULT;
  protected long maxJobSize = MAX_JOB_SIZE_DEFAULT;

  @Override
  public String name() {
    return "Binning Size Based Compaction Strategy";
  }

  @Override
  public Set<String> validOptions() {
    return ImmutableSet.of(TARGET_SIZE_OPTION, TARGET_THRESHOLD_OPTION, TARGET_MAX_JOB_SIZE_OPTION);
  }

  @Override
  public Iterator<FileScanTask> filesToCompact(Iterator<FileScanTask> dataFiles) {
    return streamOf(dataFiles)
        .filter(task -> Math.abs(task.file().fileSizeInBytes() - targetSize) > targetThreshold)
        .iterator();
  }

  @Override
  public Iterator<List<FileScanTask>> groupsFilesIntoJobs(Iterator<FileScanTask> dataFiles) {
    BinPacking.ListPacker<FileScanTask> packer =
        new BinPacking.ListPacker<>(maxJobSize, 1, false);
    List<List<FileScanTask>> bins = packer.pack(() -> dataFiles, fileScanTask -> fileScanTask.file().fileSizeInBytes());
    return bins.iterator();
  }

}
