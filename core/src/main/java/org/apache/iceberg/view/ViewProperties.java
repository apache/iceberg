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
package org.apache.iceberg.view;

/** View properties that can be set during CREATE/REPLACE view or using updateProperties API. */
public class ViewProperties {
  public static final String VERSION_HISTORY_SIZE = "version.history.num-entries";
  public static final int VERSION_HISTORY_SIZE_DEFAULT = 10;

  public static final String METADATA_COMPRESSION = "write.metadata.compression-codec";
  public static final String METADATA_COMPRESSION_DEFAULT = "gzip";
  public static final String COMMENT = "comment";
  public static final String REPLACE_DROP_DIALECT_ALLOWED = "replace.drop-dialect.allowed";
  public static final boolean REPLACE_DROP_DIALECT_ALLOWED_DEFAULT = false;

  /**
   * The mode that query engines should use to access the view.
   *
   * <p>This value is only advisory, which means that engines that can enforce the mode should do
   * so, and engines that cannot enforce the mode can choose to ignore this property, but should
   * ideally fail the query or provide a warning or provide both behaviors through an engine-level
   * feature flag
   */
  public static final String READ_ADVISORY_MODE = "read.advisory-mode";

  /**
   * The open mode advises that a view can be fully blended into the query plan and any optimization
   * can be applied against it. This typically leads to the most performant execution, but is
   * subject to side-channel attacks.
   *
   * <p>For example, a view "SELECT * FROM foo WHERE id=1" and a query "SELECT * FROM view WHERE
   * category='c1' AND user='Jack'" can be evaluated under open mode to "SELECT * FROM foo WHERE
   * id=1 AND category='c1' AND user='Jack'", but filter "user='Jack'" might be subject to
   * side-channel attacks.
   */
  public static final String READ_ADVISORY_MODE_OPEN = "open";

  /**
   * The protected mode advises that a query engine should only perform optimizations that are not
   * subject to side-channel attacks. However, attackers might find innovative ways to perform the
   * attack and it is the query engine's responsibility to keep updating its optimization.
   *
   * <p>For example, a view "SELECT * FROM foo WHERE id=1" and a query "SELECT * FROM view WHERE
   * category='c1' AND user='Jack'" can be evaluated under protected mode to "SELECT * FROM foo
   * WHERE id=1 AND category='c1'", And then the result is further filtered against "user='Jack'"
   * because the query engine considers filter "user='Jack'" as unsafe to pushdown to the view.
   */
  public static final String READ_ADVISORY_MODE_PROTECTED = "protected";

  /**
   * The isolated mode advises that a query engine should treat the view as a fully isolated
   * execution boundary. The view should be evaluated as is and no optimization should be applied by
   * the engine. This minimizes any side-channel attacks, but might lead to bad performance.
   *
   * <p>For example, a view "SELECT * FROM foo WHERE id=1" and a query "SELECT * FROM view WHERE
   * category='c1' AND user='Jack'" should be executed as first running "SELECT * FROM foo WHERE
   * id=1" And then the result is further filtered against "category='c1' AND user='Jack'" because
   * all the filters are considered potentially unsafe to pushdown to the view.
   */
  public static final String READ_ADVISORY_MODE_ISOLATED = "isolated";

  public static final String READ_ADVISORY_MODE_DEFAULT = READ_ADVISORY_MODE_OPEN;

  private ViewProperties() {}
}
