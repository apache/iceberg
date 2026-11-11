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
package org.apache.iceberg.tornadovm.bench;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Writes one equality-delete file over a BIGINT column of a table, with the keys of a text file,
 * one a line -- as a CDC writer such as Flink's would for deleted orders.
 *
 * <pre>AddEqualityDeletes &lt;table location&gt; &lt;column&gt; &lt;keys file&gt;</pre>
 */
public final class AddEqualityDeletes {

  private AddEqualityDeletes() {}

  public static void main(String[] args) throws IOException {
    Table table = MorTables.load(args[0]);
    // Keys as the column's type: INT columns (TPC-H keys from some generators) take Integers.
    boolean intKey =
        table.schema().findField(args[1]).type().typeId()
            == org.apache.iceberg.types.Type.TypeID.INTEGER;
    List<Object> keys = Lists.newArrayList();
    for (String line : Files.readAllLines(Paths.get(args[2]))) {
      if (!line.isBlank()) {
        long key = Long.parseLong(line.trim());
        keys.add(intKey ? (Object) Math.toIntExact(key) : (Object) key);
      }
    }
    MorTables.deleteEqual(table, args[1], keys);
    System.out.printf("%s: %d equality deletes on %s%n", args[0], keys.size(), args[1]);
  }
}
