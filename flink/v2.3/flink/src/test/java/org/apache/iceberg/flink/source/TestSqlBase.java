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
package org.apache.iceberg.flink.source;

import static org.apache.iceberg.flink.TestFixtures.DATABASE;

import java.io.IOException;
import java.nio.file.Path;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.TestFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/** Base class for SQL tests, providing a mini cluster, a Hadoop catalog and table environments. */
public abstract class TestSqlBase {
  @RegisterExtension
  public static MiniClusterExtension miniClusterExtension =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  @RegisterExtension
  public static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension(DATABASE, TestFixtures.TABLE);

  @TempDir protected Path temporaryFolder;

  private volatile TableEnvironment tEnv;

  private volatile TableEnvironment streamingTEnv;

  protected TableEnvironment getTableEnv() {
    if (tEnv == null) {
      synchronized (this) {
        if (tEnv == null) {
          this.tEnv =
              TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        }
      }
    }
    return tEnv;
  }

  protected TableEnvironment getStreamingTableEnv() {
    if (streamingTEnv == null) {
      synchronized (this) {
        if (streamingTEnv == null) {
          this.streamingTEnv =
              TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
        }
      }
    }

    return streamingTEnv;
  }

  @BeforeEach
  public abstract void before() throws IOException;
}
