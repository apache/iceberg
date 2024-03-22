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

import java.io.IOException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.s3a.S3ABlockOutputStream;
import org.apache.iceberg.io.PositionOutputStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class TestHadoopStreams {

  @Test
  void closeShouldThrowIOExceptionWhenInterrupted() throws IOException {

    FSDataOutputStream fsDataOutputStream =
        new FSDataOutputStream(new S3ABlockOutputStream(), null);
    PositionOutputStream wrap = HadoopStreams.wrap(fsDataOutputStream);

    // mock interrupt in S3ABlockOutputStream#putObject
    Thread.currentThread().interrupt();

    Assertions.assertThatThrownBy(wrap::close)
        .isInstanceOf(IOException.class)
        .hasMessage("S3ABlockOutputStream failed to upload object after stream was closed");
  }
}
