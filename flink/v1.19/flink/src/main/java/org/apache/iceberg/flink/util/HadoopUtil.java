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
package org.apache.iceberg.flink.util;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;

public class HadoopUtil {

  private HadoopUtil() {}

  private static final String CORE_SITE = "core-site.xml";
  private static final String HIVE_SITE = "hive-site.xml";
  private static final String HDFS_SITE = "hdfs-site.xml";

  public static Configuration mergeHiveConf(
      Object hadoopConf, String hiveConfDir, String hadoopConfDir) {
    Preconditions.checkNotNull(
        hadoopConf,
        "Hadoop configuration is null, are the hadoop dependencies available on the classpath?");

    Configuration newConf = new Configuration((Configuration) hadoopConf);
    if (!Strings.isNullOrEmpty(hiveConfDir)) {
      Preconditions.checkState(
          Files.exists(Paths.get(hiveConfDir, HIVE_SITE)),
          "There should be a hive-site.xml file under the directory %s",
          hiveConfDir);
      newConf.addResource(new Path(hiveConfDir, HIVE_SITE));
    } else {
      // If don't provide the hive-site.xml path explicitly, it will try to load resource from
      // classpath. If still
      // couldn't load the configuration file, then it will throw exception in HiveCatalog.
      URL configFile = CatalogLoader.class.getClassLoader().getResource(HIVE_SITE);
      if (configFile != null) {
        newConf.addResource(configFile);
      }
    }

    if (!Strings.isNullOrEmpty(hadoopConfDir)) {
      Preconditions.checkState(
          Files.exists(Paths.get(hadoopConfDir, HDFS_SITE)),
          "Failed to load Hadoop configuration: missing %s",
          Paths.get(hadoopConfDir, HDFS_SITE));
      newConf.addResource(new Path(hadoopConfDir, HDFS_SITE));
      Preconditions.checkState(
          Files.exists(Paths.get(hadoopConfDir, CORE_SITE)),
          "Failed to load Hadoop configuration: missing %s",
          Paths.get(hadoopConfDir, CORE_SITE));
      newConf.addResource(new Path(hadoopConfDir, CORE_SITE));
    }

    return newConf;
  }
}
