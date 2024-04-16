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
package org.apache.iceberg.flink;

import java.io.File;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;

public class TestTableLoader implements TableLoader {
  private File dir;

  public static TableLoader of(String dir) {
    return new TestTableLoader(dir);
  }

  public TestTableLoader(String dir) {
    this.dir = new File(dir);
  }

  @Override
  public void open() {}

  @Override
  public boolean isOpen() {
    return true;
  }

  @Override
  public Table loadTable() {
    return TestTables.load(dir, "test");
  }

  @Override
  @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
  public TableLoader clone() {
    return new TestTableLoader(dir.getAbsolutePath());
  }

  @Override
  public void close() {}
}
