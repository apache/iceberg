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
package org.apache.iceberg.orc;

import java.io.IOException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.io.InputFile;
import org.apache.orc.impl.OrcTail;
import org.apache.orc.impl.ReaderImpl;

/**
 * Utilities that rely on Iceberg code from org.apache.iceberg.orc package and are required for ORC
 * vectorization.
 */
public class VectorizedReadUtils {

  private VectorizedReadUtils() {}

  /**
   * Opens the ORC inputFile and reads the metadata information to construct the OrcTail content.
   * Unfortunately the API doesn't allow simple access to OrcTail, so we need the serialization
   * trick.
   *
   * @param inputFile - the ORC file
   * @param job - JobConf instance for the current task
   * @throws IOException - errors relating to accessing the ORC file
   */
  public static OrcTail getOrcTail(InputFile inputFile, JobConf job) throws IOException {

    try (ReaderImpl orcFileReader = (ReaderImpl) ORC.newFileReader(inputFile, job)) {
      return ReaderImpl.extractFileTail(orcFileReader.getSerializedFileFooter());
    }
  }
}
