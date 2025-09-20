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

import org.apache.iceberg.data.parquet.InternalReader;
import org.apache.iceberg.data.parquet.InternalWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;

public class InternalParquet {
  private InternalParquet() {}

  public static void register() {
    InternalData.register(
        FileFormat.PARQUET, InternalParquet::writeInternal, InternalParquet::readInternal);
  }

  private static Parquet.WriteBuilder writeInternal(OutputFile outputFile) {
    return Parquet.write(outputFile).createWriterFunc(InternalWriter::createWriter);
  }

  private static Parquet.ReadBuilder readInternal(InputFile inputFile) {
    return Parquet.read(inputFile).createReaderFunc(InternalReader.readerFunction());
  }
}
