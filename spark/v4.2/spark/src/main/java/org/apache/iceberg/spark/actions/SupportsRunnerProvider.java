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
package org.apache.iceberg.spark.actions;

/**
 * A rewrite runner whose work a {@link FileRewriteRunnerProvider} may take over.
 *
 * <p>A provided runner only promises to rewrite each group's live rows into new data files, as
 * bin-pack does. Runners that sort or cluster the rows (sort, z-order) promise more and do not
 * implement this, so the action never replaces them.
 */
interface SupportsRunnerProvider {}
