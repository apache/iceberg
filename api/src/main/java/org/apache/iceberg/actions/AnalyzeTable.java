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
package org.apache.iceberg.actions;

import java.util.List;
import java.util.Set;

/** An action that collects statistics of an Iceberg table and writes to Puffin files. */
public interface AnalyzeTable extends Action<AnalyzeTable, AnalyzeTable.Result> {
  /**
   * The set of columns to be analyzed
   *
   * @param columns a set of column names to be analyzed
   * @return this for method chaining
   */
  AnalyzeTable columns(Set<String> columns);

  /**
   * A set of statistics to be collected on the given columns of the given table
   *
   * @param statsToBeCollected set of statistics to be collected
   * @return this for method chaining
   */
  AnalyzeTable stats(Set<String> statsToBeCollected);

  /**
   * id of the snapshot for which stats needs to be collected
   *
   * @param snapshotId long id of the snapshot for which stats needs to be collected
   * @return this for method chaining
   */
  AnalyzeTable snapshot(String snapshotId);

  /** The action result that contains a summary of the Analysis. */
  interface Result {
    /** Returns summary of analysis */
    List<AnalysisResult> analysisResults();
  }

  interface AnalysisResult {
    /** Returns the name of statistic */
    String statsName();

    /** Returns if the stats was collected successfully */
    boolean statsCollected();

    /** Returns the errors from collecting the statistics */
    List<String> errors();
  }
}
