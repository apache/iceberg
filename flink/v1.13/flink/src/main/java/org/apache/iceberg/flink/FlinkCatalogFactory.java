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

package org.apache.iceberg.flink;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * A Flink Catalog factory implementation that creates {@link FlinkCatalog}.
 *
 * <p>This supports the following catalog configuration options:
 *
 * <ul>
 *   <li><code>type</code> - Flink catalog factory key, should be "iceberg"
 *   <li><code>catalog-type</code> - iceberg catalog type, "hive" or "hadoop"
 *   <li><code>uri</code> - the Hive Metastore URI (Hive catalog only)
 *   <li><code>clients</code> - the Hive Client Pool Size (Hive catalog only)
 *   <li><code>warehouse</code> - the warehouse path (Hadoop catalog only)
 *   <li><code>default-database</code> - a database name to use as the default
 *   <li><code>base-namespace</code> - a base namespace as the prefix for all databases (Hadoop
 *       catalog only)
 *   <li><code>cache-enabled</code> - whether to enable catalog cache
 * </ul>
 *
 * <p>To use a custom catalog that is not a Hive or Hadoop catalog, extend this class and override
 * {@link #createCatalogLoader(String, Configuration, Object)}.
 */
public class FlinkCatalogFactory implements CatalogFactory {

  /**
   * Create an Iceberg {@link org.apache.iceberg.catalog.Catalog} loader to be used by this Flink
   * catalog adapter.
   *
   * @param name Flink's catalog name
   * @param config Flink's catalog properties
   * @return an Iceberg catalog loader
   */
  static CatalogLoader createCatalogLoader(String name, Configuration config, Object hadoopConf) {

    String catalogImpl = config.get(FlinkCatalogFactoryOptions.CATALOG_IMPL);
    String catalogType = config.get(FlinkCatalogFactoryOptions.ICEBERG_CATALOG_TYPE);

    if (catalogImpl != null) {
      Preconditions.checkArgument(
          // this checks if the "catalog-type" option is explicitly set regardless of
          // `.defaultValue`
          !config.contains(FlinkCatalogFactoryOptions.ICEBERG_CATALOG_TYPE),
          "Cannot create catalog %s, both catalog-type and catalog-impl are set: catalog-type=%s, catalog-impl=%s",
          name,
          catalogType,
          catalogImpl);
      return CatalogLoader.custom(name, config.toMap(), hadoopConf, catalogImpl);
    }

    switch (catalogType) {
      case CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE:
        return CatalogLoader.hive(name, hadoopConf, config.toMap());

      case CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP:
        return CatalogLoader.hadoop(name, hadoopConf, config.toMap());

      default:
        throw new UnsupportedOperationException(
            "Unknown catalog-type: " + catalogType + " (Must be 'hive' or 'hadoop')");
    }
  }


  public static Object clusterHadoopConf(ClassLoader classLoader) {
    Class<?> hadoopUtils;
    try {
      hadoopUtils = classLoader.loadClass("org.apache.flink.runtime.util.HadoopUtils");
    } catch (ClassNotFoundException e) {
      return null;
    }

    try {
      Method getHadoopConfiguration =
          hadoopUtils.getMethod("getHadoopConfiguration", Configuration.class);
      return getHadoopConfiguration.invoke(null, GlobalConfiguration.loadConfiguration());
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      // this shouldn't happen because HadoopUtils was loaded and defines getHadoopConfiguration
      throw new UnsupportedOperationException(
          "Failed to load HadoopUtils.getHadoopConfiguration after loading HadoopUtils", e);
    }
  }

  @Override
  public Catalog createCatalog(Context context) {
    final FactoryUtil.CatalogFactoryHelper helper =
        FactoryUtil.createCatalogFactoryHelper(this, context);
    FactoryUtil.validateFactoryOptions(this, helper.getOptions());

    Object hadoopConf = clusterHadoopConf(context.getClassLoader());

    CatalogLoader catalogLoader =
        createCatalogLoader(context.getName(), (Configuration) helper.getOptions(), hadoopConf);

    Optional<String> maybeBaseNamespace =
        helper.getOptions().getOptional(FlinkCatalogFactoryOptions.BASE_NAMESPACE);
    Namespace baseNamespace =
        maybeBaseNamespace.isPresent() ?
            Namespace.of(maybeBaseNamespace.get().split("\\.")) :
            Namespace.empty();

    return new FlinkCatalog(
        context.getName(),
        helper.getOptions().get(FlinkCatalogFactoryOptions.DEFAULT_DATABASE),
        baseNamespace,
        catalogLoader,
        helper.getOptions().get(FlinkCatalogFactoryOptions.CACHE_ENABLED));
  }

  @Override
  public String factoryIdentifier() {
    return FlinkCatalogFactoryOptions.IDENTIFIER;
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    final Set<ConfigOption<?>> options = Sets.newHashSet();
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    final Set<ConfigOption<?>> options = Sets.newHashSet();
    options.add(FactoryUtil.PROPERTY_VERSION);
    options.add(FlinkCatalogFactoryOptions.DEFAULT_DATABASE);
    options.add(FlinkCatalogFactoryOptions.CACHE_ENABLED);
    options.add(FlinkCatalogFactoryOptions.BASE_NAMESPACE);
    options.add(FlinkCatalogFactoryOptions.CATALOG_IMPL);
    options.add(FlinkCatalogFactoryOptions.ICEBERG_CATALOG_TYPE);
    options.add(FlinkCatalogFactoryOptions.HIVE_CONF_DIR);

    return options;
  }
}
