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
package org.apache.iceberg.spark.source;

import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.SerializableSupplier;
import org.apache.spark.util.KnownSizeEstimation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SerializableFileIOWithSize
    implements FileIO, HadoopConfigurable, KnownSizeEstimation, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(SerializableFileIOWithSize.class);
  private static final long SIZE_ESTIMATE = 32_768L;

  private final transient Object serializationMarker;

  private final FileIO delegate;

  private SerializableFileIOWithSize(FileIO fileIO) {
    this.delegate = fileIO;
    this.serializationMarker = new Object();
  }

  @Override
  public long estimatedSize() {
    return SIZE_ESTIMATE;
  }

  public static FileIO copyOf(FileIO fileIO) {
    return new SerializableFileIOWithSize(SerializableTable.copyOf(fileIO));
  }

  @Override
  public void close() {
    if (null == serializationMarker) {
      LOG.info("Closing FileIO");
      delegate.close();
    }
  }

  protected FileIO io() {
    return delegate;
  }

  @Override
  public InputFile newInputFile(String path) {
    return delegate.newInputFile(path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return delegate.newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    delegate.deleteFile(path);
  }

  @Override
  public void initialize(Map<String, String> properties) {
    delegate.initialize(properties);
  }

  @Override
  public Map<String, String> properties() {
    return delegate.properties();
  }

  @Override
  public void serializeConfWith(
      Function<Configuration, SerializableSupplier<Configuration>> confSerializer) {
    if (delegate instanceof HadoopConfigurable hadoopConfigurable) {
      hadoopConfigurable.serializeConfWith(confSerializer);
    }
  }

  @Override
  public void setConf(Configuration conf) {
    if (delegate instanceof HadoopConfigurable hadoopConfigurable) {
      hadoopConfigurable.setConf(conf);
    }
  }

  @Override
  public Configuration getConf() {
    if (delegate instanceof HadoopConfigurable hadoopConfigurable) {
      return hadoopConfigurable.getConf();
    }
    return null;
  }
}
