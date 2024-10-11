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
package org.apache.iceberg.mr.mapreduce;

import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.util.SerializationUtil;

public class Utils {

  private Utils() {}

  /**
   * Returns the Table serialized to the configuration based on the table name. If configuration is
   * missing from the FileIO of the table, it will be populated with the input config.
   *
   * @param config The configuration used to get the data from
   * @param name The name of the table we need as returned by TableDesc.getTableName()
   * @return The Table
   */
  public static Table table(Configuration config, String name) {
    Table table =
        SerializationUtil.deserializeFromBase64(
            config.get(InputFormatConfig.SERIALIZED_TABLE_PREFIX + name));
    checkAndSetIoConfig(config, table);
    return table;
  }

  /**
   * If enabled, it populates the FileIO's hadoop configuration with the input config object. This
   * might be necessary when the table object was serialized without the FileIO config.
   *
   * @param config Configuration to set for FileIO, if enabled
   * @param table The Iceberg table object
   */
  public static void checkAndSetIoConfig(Configuration config, Table table) {
    if (table != null
        && config.getBoolean(
            InputFormatConfig.CONFIG_SERIALIZATION_DISABLED,
            InputFormatConfig.CONFIG_SERIALIZATION_DISABLED_DEFAULT)
        && table.io() instanceof HadoopConfigurable) {
      ((HadoopConfigurable) table.io()).setConf(config);
    }
  }

  /**
   * If enabled, it ensures that the FileIO's hadoop configuration will not be serialized. This
   * might be desirable for decreasing the overall size of serialized table objects.
   *
   * <p>Note: Skipping FileIO config serialization in this fashion might in turn necessitate calling
   * {@link #checkAndSetIoConfig(Configuration, Table)} on the deserializer-side to enable
   * subsequent use of the FileIO.
   *
   * @param config Configuration to set for FileIO in a transient manner, if enabled
   * @param table The Iceberg table object
   */
  public static void checkAndSkipIoConfigSerialization(Configuration config, Table table) {
    if (table != null
        && config.getBoolean(
            InputFormatConfig.CONFIG_SERIALIZATION_DISABLED,
            InputFormatConfig.CONFIG_SERIALIZATION_DISABLED_DEFAULT)
        && table.io() instanceof HadoopConfigurable) {
      ((HadoopConfigurable) table.io())
          .serializeConfWith(conf -> new NonSerializingConfig(config)::get);
    }
  }

  private static class NonSerializingConfig implements Serializable {

    private final transient Configuration conf;

    NonSerializingConfig(Configuration conf) {
      this.conf = conf;
    }

    public Configuration get() {
      if (conf == null) {
        throw new IllegalStateException(
            "Configuration was not serialized on purpose but was not set manually either");
      }

      return conf;
    }
  }
}
