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
package org.apache.iceberg.data.orc;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.orc.ORCSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestEmptyStructRepro {
  @TempDir private Path temp;

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "empty", Types.StructType.of()));

  @Test
  public void debugProjection() throws Exception {
    GenericRecord rec = GenericRecord.create(SCHEMA);
    rec.setField("id", 1L);
    rec.setField("empty", GenericRecord.create(Types.StructType.of()));

    File testFile = temp.resolve("empty-struct-debug.orc").toFile();
    try (FileAppender<Record> writer =
        ORC.write(Files.localOutput(testFile))
            .schema(SCHEMA)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .build()) {
      writer.add(rec);
    }

    org.apache.orc.Reader orcReader =
        OrcFile.createReader(
            new org.apache.hadoop.fs.Path(testFile.getAbsolutePath()),
            OrcFile.readerOptions(new Configuration()));
    TypeDescription fileSchema = orcReader.getSchema();
    System.out.println("FILE SCHEMA: " + fileSchema);
    for (int i = 0; i < fileSchema.getChildren().size(); i++) {
      TypeDescription child = fileSchema.getChildren().get(i);
      System.out.println("  CHILD[" + i + "]: " + child + " category=" + child.getCategory() + " children.size=" + (child.getChildren() != null ? child.getChildren().size() : "null"));
    }

    TypeDescription projection = ORCSchemaUtil.buildOrcProjection(SCHEMA, fileSchema);
    System.out.println("PROJECTION: " + projection);
    for (int i = 0; i < projection.getChildren().size(); i++) {
      TypeDescription child = projection.getChildren().get(i);
      System.out.println("  CHILD[" + i + "]: " + child + " category=" + child.getCategory() + " children.size=" + (child.getChildren() != null ? child.getChildren().size() : "null"));
    }
  }
}