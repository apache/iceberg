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

import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SerializableSupplier;

public class SerializableFileIO implements FileIO, HadoopConfigurable {
  private final FileIO delegate;

  public SerializableFileIO(FileIO delegate) {
    Preconditions.checkArgument(delegate != null, "Invalid FileIO: null");
    this.delegate = delegate;
  }

  public static FileIO copyOf(FileIO fileIO) {
    if (fileIO instanceof HadoopConfigurable) {
      ((HadoopConfigurable) fileIO).serializeConfWith(SerializableConfSupplier::new);
    }

    return new SerializableFileIO(fileIO);
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
  public void close() {
    delegate.close();
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

  // captures the current state of a Hadoop configuration in a serializable manner
  private static class SerializableConfSupplier implements SerializableSupplier<Configuration> {
    private final Map<String, String> confAsMap;
    private transient volatile Configuration conf = null;

    private SerializableConfSupplier(Configuration conf) {
      this.confAsMap = Maps.newHashMapWithExpectedSize(conf.size());
      conf.forEach(entry -> confAsMap.put(entry.getKey(), entry.getValue()));
    }

    @Override
    public Configuration get() {
      if (conf == null) {
        synchronized (this) {
          if (conf == null) {
            Configuration newConf = new Configuration(false);
            confAsMap.forEach(newConf::set);
            this.conf = newConf;
          }
        }
      }

      return conf;
    }
  }
}
