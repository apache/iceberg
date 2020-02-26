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
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.orc.Reader;
import org.apache.orc.Writer;

public class OrcMetrics {

  private OrcMetrics() {
  }

  public static Metrics fromInputFile(InputFile file) {
    final Configuration config = (file instanceof HadoopInputFile) ?
        ((HadoopInputFile) file).getConf() : new Configuration();
    return fromInputFile(file, config);
  }

  public static Metrics fromInputFile(InputFile file, Configuration config) {
    try (Reader orcReader = ORC.newFileReader(file, config)) {

      // TODO: implement rest of the methods for ORC metrics
      // https://github.com/apache/incubator-iceberg/pull/199
      return new Metrics(orcReader.getNumberOfRows(),
          null,
          null,
          Collections.emptyMap(),
          null,
          null);
    } catch (IOException ioe) {
      throw new RuntimeIOException(ioe, "Failed to read footer of file: %s", file);
    }
  }

  static Metrics fromWriter(Writer writer) {
    // TODO: implement rest of the methods for ORC metrics in
    // https://github.com/apache/incubator-iceberg/pull/199
    return new Metrics(writer.getNumberOfRows(),
        null,
        null,
        Collections.emptyMap(),
        null,
        null);
  }
}
