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
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SerializableSupplier;

public class SerializableFileIO {
  private SerializableFileIO() {}

  public static FileIO copyOf(FileIO fileIO) {
    if (fileIO instanceof HadoopConfigurable) {
      ((HadoopConfigurable) fileIO).serializeConfWith(SerializableConfSupplier::new);
    }

    return fileIO;
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
