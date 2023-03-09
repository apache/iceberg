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
package org.apache.iceberg.spark;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileRewriteCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(FileRewriteCoordinator.class);
  private static final FileRewriteCoordinator INSTANCE = new FileRewriteCoordinator();

  private final Map<Pair<String, String>, Set<DataFile>> resultMap = Maps.newConcurrentMap();

  private FileRewriteCoordinator() {}

  public static FileRewriteCoordinator get() {
    return INSTANCE;
  }

  /**
   * Called to persist the output of a rewrite action for a specific group. Since the write is done
   * via a Spark Datasource, we have to propagate the result through this side-effect call.
   *
   * @param table table where the rewrite is occurring
   * @param fileSetID the id used to identify the source set of files being rewritten
   * @param newDataFiles the new files which have been written
   */
  public void stageRewrite(Table table, String fileSetID, Set<DataFile> newDataFiles) {
    LOG.debug(
        "Staging the output for {} - fileset {} with {} files",
        table.name(),
        fileSetID,
        newDataFiles.size());
    Pair<String, String> id = toID(table, fileSetID);
    resultMap.put(id, newDataFiles);
  }

  public Set<DataFile> fetchNewDataFiles(Table table, String fileSetID) {
    Pair<String, String> id = toID(table, fileSetID);
    Set<DataFile> result = resultMap.get(id);
    ValidationException.check(
        result != null, "No results for rewrite of file set %s in table %s", fileSetID, table);

    return result;
  }

  public void clearRewrite(Table table, String fileSetID) {
    LOG.debug("Removing entry from RewriteCoordinator for {} - id {}", table.name(), fileSetID);
    Pair<String, String> id = toID(table, fileSetID);
    resultMap.remove(id);
  }

  public Set<String> fetchSetIDs(Table table) {
    return resultMap.keySet().stream()
        .filter(e -> e.first().equals(tableUUID(table)))
        .map(Pair::second)
        .collect(Collectors.toSet());
  }

  private Pair<String, String> toID(Table table, String setID) {
    String tableUUID = tableUUID(table);
    return Pair.of(tableUUID, setID);
  }

  private String tableUUID(Table table) {
    TableOperations ops = ((HasTableOperations) table).operations();
    return ops.current().uuid();
  }
}
