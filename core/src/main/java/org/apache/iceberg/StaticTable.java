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

package org.apache.iceberg;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import org.apache.iceberg.io.FileIO;

/**
 * Table implementation that provides access to metadata for a given snapshot of the Table using a
 * table metadata location. It will never refer to a different Metadata object than the one it was created with
 * and cannot be used to create or delete files.
 * This table could be serialized and deserialized.
 */
public class StaticTable extends BaseTable {
  public StaticTable(FileIO io, String name, String metadataLocation) {
    super(new StaticTableOperations(metadataLocation, io), name);
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.writeObject(io());
    out.writeObject(name());
    out.writeObject(operations().current().metadataFileLocation());
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    FileIO io = (FileIO) in.readObject();
    String name = (String) in.readObject();
    String metadataLocation = (String) in.readObject();
    init(new StaticTableOperations(metadataLocation, io), name);
  }

  private void readObjectNoData() throws ObjectStreamException {
    throw new UnsupportedOperationException("Deserializing empty StaticTable is not supported");
  }

  @Override
  Object writeReplace() throws ObjectStreamException {
    return this;
  }
}
