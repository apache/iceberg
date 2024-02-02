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
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class BaseFileRewriteCoordinator<F extends ContentFile<F>> {

  private static final Logger LOG = LoggerFactory.getLogger(BaseFileRewriteCoordinator.class);

  private final Map<Pair<String, String>, Set<F>> resultMap = Maps.newConcurrentMap();

  /**
   * Called to persist the output of a rewrite action for a specific group. Since the write is done
   * via a Spark Datasource, we have to propagate the result through this side-effect call.
   *
   * @param table table where the rewrite is occurring
   * @param fileSetId the id used to identify the source set of files being rewritten
   * @param newFiles the new files which have been written
   */
  public void stageRewrite(Table table, String fileSetId, Set<F> newFiles) {
    LOG.debug(
        "Staging the output for {} - fileset {} with {} files",
        table.name(),
        fileSetId,
        newFiles.size());
    Pair<String, String> id = toId(table, fileSetId);
    resultMap.put(id, newFiles);
  }

  public Set<F> fetchNewFiles(Table table, String fileSetId) {
    Pair<String, String> id = toId(table, fileSetId);
    Set<F> result = resultMap.get(id);
    ValidationException.check(
        result != null, "No results for rewrite of file set %s in table %s", fileSetId, table);

    return result;
  }

  public void clearRewrite(Table table, String fileSetId) {
    LOG.debug("Removing entry for {} - id {}", table.name(), fileSetId);
    Pair<String, String> id = toId(table, fileSetId);
    resultMap.remove(id);
  }

  public Set<String> fetchSetIds(Table table) {
    return resultMap.keySet().stream()
        .filter(e -> e.first().equals(Spark3Util.baseTableUUID(table)))
        .map(Pair::second)
        .collect(Collectors.toSet());
  }

  private Pair<String, String> toId(Table table, String setId) {
    return Pair.of(Spark3Util.baseTableUUID(table), setId);
  }
}
