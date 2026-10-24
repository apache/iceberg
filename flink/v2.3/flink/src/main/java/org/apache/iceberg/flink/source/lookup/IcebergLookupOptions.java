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
package org.apache.iceberg.flink.source.lookup;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Lookup join options for the Iceberg table source. */
@Internal
public class IcebergLookupOptions {

  public static final ConfigOption<Boolean> FULL_CACHE_EAGER_LOAD =
      ConfigOptions.key("lookup.full-cache.eager-load")
          .booleanType()
          .defaultValue(true)
          .withDescription(
              "Whether to load the full lookup cache when the lookup function is opened, instead "
                  + "of on the first lookup. Eager loading narrows the window in which the subtasks "
                  + "of the join can end up on different snapshots of the dimension table, and "
                  + "fails the job at startup if the table cannot be read, at the cost of a longer "
                  + "deployment. When it is disabled, a configured reload only refreshes the cache "
                  + "after the first lookup has loaded it.");

  private IcebergLookupOptions() {}
}
