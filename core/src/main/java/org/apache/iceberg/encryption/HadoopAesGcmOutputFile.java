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
package org.apache.iceberg.encryption;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.hadoop.HasConfiguration;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class HadoopAesGcmOutputFile extends AesGcmOutputFile
    implements HasConfiguration<Configuration> {
  private final Configuration conf;

  public HadoopAesGcmOutputFile(OutputFile targetFile, byte[] dataKey, byte[] fileAADPrefix) {
    super(targetFile, dataKey, fileAADPrefix);
    Preconditions.checkArgument(
        targetFile instanceof HadoopOutputFile,
        "HadoopAesGcmOutputFile requires a HadoopOutputFile");
    this.conf = ((HadoopOutputFile) targetFile).getConf();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
