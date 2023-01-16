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
package org.apache.iceberg.hadoop;

import java.io.Serializable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * A {@link PathFilter} that filters out hidden paths. A path is considered to be hidden when the
 * path name starts with a period ('.') or an underscore ('_').
 */
public class HiddenPathFilter implements PathFilter, Serializable {

  private static final HiddenPathFilter INSTANCE = new HiddenPathFilter();

  private HiddenPathFilter() {}

  public static HiddenPathFilter get() {
    return INSTANCE;
  }

  @Override
  public boolean accept(Path p) {
    return !p.getName().startsWith("_") && !p.getName().startsWith(".");
  }
}
