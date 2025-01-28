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
package org.apache.iceberg.connect;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.common.DynMethods.BoundMethod;
import org.apache.iceberg.common.DynMethods.StaticMethod;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CatalogUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogUtils.class.getName());
  private static final List<String> HADOOP_CONF_FILES =
      ImmutableList.of("core-site.xml", "hdfs-site.xml", "hive-site.xml");

  static Catalog loadCatalog(IcebergSinkConfig config) {
    Object hadoopConf = loadHadoopConfig(config);
    if (config.kerberosAuthentication()) {
      configureKerberosAuthentication(
          hadoopConf,
          config.connectHdfsPrincipal(),
          config.connectHdfsKeytab(),
          config.kerberosTicketRenewPeriodMs());
    }
    return CatalogUtil.buildIcebergCatalog(config.catalogName(), config.catalogProps(), hadoopConf);
  }

  // use reflection here to avoid requiring Hadoop as a dependency
  private static Object loadHadoopConfig(IcebergSinkConfig config) {
    Class<?> configClass =
        DynClasses.builder()
            .impl("org.apache.hadoop.hdfs.HdfsConfiguration")
            .impl("org.apache.hadoop.conf.Configuration")
            .orNull()
            .build();

    if (configClass == null) {
      LOG.info("Hadoop not found on classpath, not creating Hadoop config");
      return null;
    }

    try {
      Object result = DynConstructors.builder().hiddenImpl(configClass).build().newInstance();
      BoundMethod addResourceMethod =
          DynMethods.builder("addResource").impl(configClass, URL.class).build(result);
      BoundMethod setMethod =
          DynMethods.builder("set").impl(configClass, String.class, String.class).build(result);

      //  load any config files in the specified config directory
      String hadoopConfDir = config.hadoopConfDir();
      if (hadoopConfDir != null) {
        HADOOP_CONF_FILES.forEach(
            confFile -> {
              Path path = Paths.get(hadoopConfDir, confFile);
              if (Files.exists(path)) {
                try {
                  addResourceMethod.invoke(path.toUri().toURL());
                } catch (IOException e) {
                  LOG.warn("Error adding Hadoop resource {}, resource was not added", path, e);
                }
              }
            });
      }

      // set any Hadoop properties specified in the sink config
      config.hadoopProps().forEach(setMethod::invoke);

      LOG.info("Hadoop config initialized: {}", configClass.getName());
      return result;
    } catch (Exception e) {
      LOG.warn(
          "Hadoop found on classpath but could not create config, proceeding without config", e);
    }
    return null;
  }

  private static void configureKerberosAuthentication(
      Object hadoopConf, String principal, String keytabPath, long renewPeriod) {
    if (principal == null || keytabPath == null) {
      throw new ConfigException(
          "Hadoop is using Kerberos for authentication, you need to provide both a connect "
              + "principal and the path to the keytab of the principal.");
    }

    Class<?> configurationClass =
        DynClasses.builder().impl("org.apache.hadoop.conf.Configuration").build();
    Class<?> userGroupInformationClass =
        DynClasses.builder().impl("org.apache.hadoop.security.UserGroupInformation").build();
    if (configurationClass == null) {
      throw new RuntimeException("Hadoop not found on classpath, not creating Hadoop config");
    }
    if (userGroupInformationClass == null) {
      throw new RuntimeException(
          "Hadoop security not found on classpath, unable to configure Kerberos authentication");
    }

    try {
      StaticMethod setConfigurationMethod =
          DynMethods.builder("setConfiguration")
              .impl(userGroupInformationClass, configurationClass)
              .buildStatic();
      StaticMethod loginUserFromKeytabMethod =
          DynMethods.builder("loginUserFromKeytab")
              .impl(userGroupInformationClass, String.class, String.class)
              .buildStatic();
      StaticMethod getLoginUserMethod =
          DynMethods.builder("getLoginUser").impl(userGroupInformationClass).buildStatic();

      setConfigurationMethod.invoke(hadoopConf);
      loginUserFromKeytabMethod.invoke(principal, keytabPath);
      final Object ugi = getLoginUserMethod.invoke();
      BoundMethod getUserNameMethod =
          DynMethods.builder("getUserName").impl(userGroupInformationClass).build(ugi);
      String userName = getUserNameMethod.invoke();
      LOG.info("login as: {}", userName);

      Thread ticketRenewThread =
          new Thread(() -> renewKerberosTicket(userGroupInformationClass, ugi, renewPeriod));
      ticketRenewThread.setDaemon(true);
      LOG.info("Starting the Kerberos ticket renew with period {} ms.", renewPeriod);
      ticketRenewThread.start();
    } catch (Exception e) {
      throw new RuntimeException("Could not authenticate with Kerberos: " + e.getMessage());
    }
  }

  private static void renewKerberosTicket(Class<?> ugiClass, Object ugi, long renewPeriod) {
    BoundMethod reloginFromKeytabMethod =
        DynMethods.builder("reloginFromKeytab").impl(ugiClass).build(ugi);
    BoundMethod getUserNameMethod = DynMethods.builder("getUserName").impl(ugiClass).build(ugi);
    String userName = getUserNameMethod.invoke();
    while (true) {
      try {
        Thread.sleep(renewPeriod);
        LOG.info("Attempting to re-login from keytab for user {}", userName);
        reloginFromKeytabMethod.invoke();
      } catch (Exception e) {
        LOG.error("Error renewing the ticket", e);
      }
    }
  }

  private CatalogUtils() {}
}
