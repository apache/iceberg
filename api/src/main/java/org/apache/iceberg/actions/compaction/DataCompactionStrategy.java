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

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

public interface DataCompactionStrategy extends Serializable {

  /**
   * Returns the name of this compaction strategy
   */
  String name();

  /**
   * Returns a set of options which this compaction strategy can use. This is an allowed-list and any options not
   * specified here will be rejected at runtime.
   */
  Set<String> validOptions();

  default void validateOptions(Map<String, String> options) {
    Sets.SetView<String> invalidOptions = Sets.difference(options.keySet(), validOptions());
    if (!invalidOptions.isEmpty()) {
      String invalidColumnString = invalidOptions.stream().collect(Collectors.joining(",", "[", "]"));
      String validColumnString = validOptions().stream().collect(Collectors.joining(",", "[", "]"));

      throw new IllegalArgumentException(String.format(
          "Cannot use strategy %s with unknown options %s. This strategy accepts %s",
          name(), invalidColumnString, validColumnString));
    }
  }

  /**
   * Before the compaction strategy rules are applied, the underlying action has the ability to use this expression
   * to filter the FileScanTasks which will be created when planning file reads that will later be run through
   * this compaction strategy.
   *
   * @return an Iceberg expression to use when discovering file scan tasks
   */
  default Expression preFilter() {
    return Expressions.alwaysTrue();
  }

  /**
   * Removes all file references which this plan will not rewrite or change. Unlike the preFilter, this method can
   * execute arbitrary code and isn't restricted to just Iceberg Expressions. This should be serializable so that
   * Actions which run remotely can utilize the method.
   *
   * @param dataFiles iterator of live datafiles in a given partition
   * @return iterator containing only files to be rewritten
   */
  Iterator<FileScanTask> filesToCompact(Iterator<FileScanTask> dataFiles);

  /**
   * Groups file scans into lists which will be processed in a single executable
   * unit. Each group will end up being committed as an independent set of
   * changes. This creates the jobs which will eventually be run as by the underlying Action.
   *
   * @param dataFiles iterator of files to be rewritten
   * @return iterator of sets of files to be processed together
   */
  Iterator<List<FileScanTask>> groupsFilesIntoJobs(Iterator<FileScanTask> dataFiles);

  /**
   * Method which will actually rewrite and commit changes to a group of files
   * based on the particular CompactionStrategy's Algorithm. This will most likely be
   * Action framework specific.
   *
   * @param table
   * @param filesToRewrite a group of files to be rewritten together
   * @return a list of newly committed files
   */
  List<DataFile> rewriteFiles(Table table, List<FileScanTask> filesToRewrite, String description);
}
