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
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.SerializableSupplier;
import org.apache.spark.util.KnownSizeEstimation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a serializable {@link FileIO} with a known size estimate. Spark calls its
 * {@link org.apache.spark.util.SizeEstimator} class when broadcasting variables and this can be an
 * expensive operation, so providing a known size estimate allows that operation to be skipped.
 *
 * <p>This class also implements {@link AutoCloseable} to avoid leaking resources upon broadcasting.
 * Broadcast variables are destroyed and cleaned up on the driver and executors once they are
 * garbage collected on the driver. The implementation ensures only resources used by copies of the
 * main {@link FileIO} are released.
 */
class SerializableFileIOWithSize
    implements FileIO, HadoopConfigurable, KnownSizeEstimation, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(SerializableFileIOWithSize.class);
  private static final long SIZE_ESTIMATE = 32_768L;
  private final transient Object serializationMarker;
  private final FileIO fileIO;

  private SerializableFileIOWithSize(FileIO fileIO) {
    this.fileIO = fileIO;
    this.serializationMarker = new Object();
  }

  @Override
  public long estimatedSize() {
    return SIZE_ESTIMATE;
  }

  public static FileIO wrap(FileIO fileIO) {
    return new SerializableFileIOWithSize(fileIO);
  }

  @Override
  public void close() {
    if (null == serializationMarker) {
      LOG.debug("Closing FileIO");
      fileIO.close();
    }
  }

  @Override
  public InputFile newInputFile(String path) {
    return fileIO.newInputFile(path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return fileIO.newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    fileIO.deleteFile(path);
  }

  @Override
  public void initialize(Map<String, String> properties) {
    fileIO.initialize(properties);
  }

  @Override
  public Map<String, String> properties() {
    return fileIO.properties();
  }

  @Override
  public void serializeConfWith(
      Function<Configuration, SerializableSupplier<Configuration>> confSerializer) {
    if (fileIO instanceof HadoopConfigurable configurable) {
      configurable.serializeConfWith(confSerializer);
    }
  }

  @Override
  public void setConf(Configuration conf) {
    if (fileIO instanceof HadoopConfigurable configurable) {
      configurable.setConf(conf);
    }
  }

  @Override
  public Configuration getConf() {
    if (fileIO instanceof HadoopConfigurable hadoopConfigurable) {
      return hadoopConfigurable.getConf();
    }

    return null;
  }
}
