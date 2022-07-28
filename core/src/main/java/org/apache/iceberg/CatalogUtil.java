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

import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.apache.iceberg.TableProperties.GC_ENABLED_DEFAULT;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.MapMaker;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.FileIOUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogUtil {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogUtil.class);

  /**
   * Shortcut catalog property to load a catalog implementation through a short type name, instead
   * of specifying a full java class through {@link CatalogProperties#CATALOG_IMPL}. Currently the
   * following type to implementation mappings are supported:
   *
   * <ul>
   *   <li>hive: org.apache.iceberg.hive.HiveCatalog
   *   <li>hadoop: org.apache.iceberg.hadoop.HadoopCatalog
   * </ul>
   */
  public static final String ICEBERG_CATALOG_TYPE = "type";

  public static final String ICEBERG_CATALOG_TYPE_HADOOP = "hadoop";
  public static final String ICEBERG_CATALOG_TYPE_HIVE = "hive";
  public static final String ICEBERG_CATALOG_HIVE = "org.apache.iceberg.hive.HiveCatalog";
  public static final String ICEBERG_CATALOG_HADOOP = "org.apache.iceberg.hadoop.HadoopCatalog";

  private CatalogUtil() {}

  /**
   * Drops all data and metadata files referenced by TableMetadata.
   *
   * <p>This should be called by dropTable implementations to clean up table files once the table
   * has been dropped in the metastore.
   *
   * @param io a FileIO to use for deletes
   * @param metadata the last valid TableMetadata instance for a dropped table.
   */
  public static void dropTableData(FileIO io, TableMetadata metadata) {
    // Reads and deletes are done using Tasks.foreach(...).suppressFailureWhenFinished to complete
    // as much of the delete work as possible and avoid orphaned data or manifest files.

    Set<String> manifestListsToDelete = Sets.newHashSet();
    Set<ManifestFile> manifestsToDelete = Sets.newHashSet();
    for (Snapshot snapshot : metadata.snapshots()) {
      // add all manifests to the delete set because both data and delete files should be removed
      Iterables.addAll(manifestsToDelete, snapshot.allManifests(io));
      // add the manifest list to the delete set, if present
      if (snapshot.manifestListLocation() != null) {
        manifestListsToDelete.add(snapshot.manifestListLocation());
      }
    }

    LOG.info("Manifests to delete: {}", Joiner.on(", ").join(manifestsToDelete));

    // run all the deletes

    boolean gcEnabled =
        PropertyUtil.propertyAsBoolean(metadata.properties(), GC_ENABLED, GC_ENABLED_DEFAULT);

    if (gcEnabled) {
      // delete data files only if we are sure this won't corrupt other tables
      deleteFiles(io, manifestsToDelete);
    }

    FileIOUtil.bulkDelete(io, Iterables.transform(manifestsToDelete, ManifestFile::path))
        .name("manifest")
        .executeWith(ThreadPools.getWorkerPool())
        .execute();

    FileIOUtil.bulkDelete(io, manifestListsToDelete)
        .name("manifest list")
        .executeWith(ThreadPools.getWorkerPool())
        .execute();

    FileIOUtil.bulkDelete(
            io, Iterables.transform(metadata.previousFiles(), TableMetadata.MetadataLogEntry::file))
        .name("previous metadata file")
        .executeWith(ThreadPools.getWorkerPool())
        .execute();

    FileIOUtil.bulkDelete(io, metadata.metadataFileLocation())
        .name("metadata file")
        .executeWith(ThreadPools.getWorkerPool())
        .execute();
  }

  @SuppressWarnings("DangerousStringInternUsage")
  private static void deleteFiles(FileIO io, Set<ManifestFile> allManifests) {
    // keep track of deleted files in a map that can be cleaned up when memory runs low
    Map<String, Boolean> deletedFiles =
        new MapMaker().concurrencyLevel(ThreadPools.WORKER_THREAD_POOL_SIZE).weakKeys().makeMap();

    Iterable<String> removedFiles =
        Iterables.concat(
            Iterables.transform(
                allManifests,
                manifest -> {
                  try (ManifestReader<?> reader = ManifestFiles.open(manifest, io)) {
                    Iterable<String> paths =
                        // intern the file path because the weak key map uses identity (==)
                        // instead of equals
                        Iterables.transform(
                            reader.entries(), entry -> entry.file().path().toString().intern());
                    return Iterables.filter(
                        paths,
                        path -> {
                          Boolean alreadyDeleted = deletedFiles.putIfAbsent(path, true);
                          return alreadyDeleted == null || !alreadyDeleted;
                        });
                  } catch (IOException e) {
                    throw new UncheckedIOException(
                        "Failed to read manifest file: " + manifest.path(), e);
                  }
                }));

    FileIOUtil.bulkDelete(io, removedFiles)
        .name("data file")
        .executeWith(ThreadPools.getWorkerPool())
        .execute();
  }

  /**
   * Load a custom catalog implementation.
   *
   * <p>The catalog must have a no-arg constructor. If the class implements Configurable, a Hadoop
   * config will be passed using Configurable.setConf. {@link Catalog#initialize(String catalogName,
   * Map options)} is called to complete the initialization.
   *
   * @param impl catalog implementation full class name
   * @param catalogName catalog name
   * @param properties catalog properties
   * @param hadoopConf hadoop configuration if needed
   * @return initialized catalog object
   * @throws IllegalArgumentException if no-arg constructor not found or error during initialization
   */
  public static Catalog loadCatalog(
      String impl, String catalogName, Map<String, String> properties, Object hadoopConf) {
    Preconditions.checkNotNull(impl, "Cannot initialize custom Catalog, impl class name is null");
    DynConstructors.Ctor<Catalog> ctor;
    try {
      ctor = DynConstructors.builder(Catalog.class).impl(impl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize Catalog implementation %s: %s", impl, e.getMessage()),
          e);
    }

    Catalog catalog;
    try {
      catalog = ctor.newInstance();

    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize Catalog, %s does not implement Catalog.", impl), e);
    }

    configureHadoopConf(catalog, hadoopConf);

    catalog.initialize(catalogName, properties);
    return catalog;
  }

  /**
   * Build an Iceberg {@link Catalog} based on a map of catalog properties and optional Hadoop
   * configuration.
   *
   * <p>This method examines both the {@link #ICEBERG_CATALOG_TYPE} and {@link
   * CatalogProperties#CATALOG_IMPL} properties to determine the catalog implementation to load. If
   * nothing is specified for both properties, Hive catalog will be loaded by default.
   *
   * @param name catalog name
   * @param options catalog properties
   * @param conf a Hadoop Configuration
   * @return initialized catalog
   */
  public static Catalog buildIcebergCatalog(String name, Map<String, String> options, Object conf) {
    String catalogImpl = options.get(CatalogProperties.CATALOG_IMPL);
    if (catalogImpl == null) {
      String catalogType =
          PropertyUtil.propertyAsString(options, ICEBERG_CATALOG_TYPE, ICEBERG_CATALOG_TYPE_HIVE);
      switch (catalogType.toLowerCase(Locale.ENGLISH)) {
        case ICEBERG_CATALOG_TYPE_HIVE:
          catalogImpl = ICEBERG_CATALOG_HIVE;
          break;
        case ICEBERG_CATALOG_TYPE_HADOOP:
          catalogImpl = ICEBERG_CATALOG_HADOOP;
          break;
        default:
          throw new UnsupportedOperationException("Unknown catalog type: " + catalogType);
      }
    } else {
      String catalogType = options.get(ICEBERG_CATALOG_TYPE);
      Preconditions.checkArgument(
          catalogType == null,
          "Cannot create catalog %s, both type and catalog-impl are set: type=%s, catalog-impl=%s",
          name,
          catalogType,
          catalogImpl);
    }

    return CatalogUtil.loadCatalog(catalogImpl, name, options, conf);
  }

  /**
   * Load a custom {@link FileIO} implementation.
   *
   * <p>The implementation must have a no-arg constructor. If the class implements Configurable, a
   * Hadoop config will be passed using Configurable.setConf. {@link FileIO#initialize(Map
   * properties)} is called to complete the initialization.
   *
   * @param impl full class name of a custom FileIO implementation
   * @param properties used to initialize the FileIO implementation
   * @param hadoopConf a hadoop Configuration
   * @return FileIO class
   * @throws IllegalArgumentException if class path not found or right constructor not found or the
   *     loaded class cannot be cast to the given interface type
   */
  public static FileIO loadFileIO(String impl, Map<String, String> properties, Object hadoopConf) {
    LOG.info("Loading custom FileIO implementation: {}", impl);
    DynConstructors.Ctor<FileIO> ctor;
    try {
      ctor =
          DynConstructors.builder(FileIO.class)
              .loader(CatalogUtil.class.getClassLoader())
              .impl(impl)
              .buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize FileIO, missing no-arg constructor: %s", impl), e);
    }

    FileIO fileIO;
    try {
      fileIO = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize FileIO, %s does not implement FileIO.", impl), e);
    }

    configureHadoopConf(fileIO, hadoopConf);

    fileIO.initialize(properties);
    return fileIO;
  }

  /**
   * Dynamically detects whether an object is a Hadoop Configurable and calls setConf.
   *
   * @param maybeConfigurable an object that may be Configurable
   * @param conf a Configuration
   */
  @SuppressWarnings("unchecked")
  public static void configureHadoopConf(Object maybeConfigurable, Object conf) {
    Preconditions.checkArgument(maybeConfigurable != null, "Cannot configure: null Configurable");
    if (conf == null) {
      return;
    }

    if (maybeConfigurable instanceof Configurable) {
      // use the Iceberg configurable interface to pass the conf
      ((Configurable<Object>) maybeConfigurable).setConf(conf);
      return;
    }

    // try to use Hadoop's Configurable interface dynamically
    // use the classloader of the object that may be configurable
    ClassLoader maybeConfigurableLoader = maybeConfigurable.getClass().getClassLoader();

    Class<?> configurableInterface;
    try {
      // load the Configurable interface
      configurableInterface =
          DynClasses.builder()
              .loader(maybeConfigurableLoader)
              .impl("org.apache.hadoop.conf.Configurable")
              .buildChecked();
    } catch (ClassNotFoundException e) {
      // not Configurable because it was loaded and Configurable is not present in its classloader
      return;
    }

    if (!configurableInterface.isInstance(maybeConfigurable)) {
      // not Configurable because the object does not implement the Configurable interface
      return;
    }

    Class<?> configurationClass;
    try {
      configurationClass =
          DynClasses.builder()
              .loader(maybeConfigurableLoader)
              .impl("org.apache.hadoop.conf.Configuration")
              .buildChecked();
    } catch (ClassNotFoundException e) {
      // this shouldn't happen because Configurable cannot be loaded without first loading
      // Configuration
      throw new UnsupportedOperationException(
          "Failed to load Configuration after loading Configurable", e);
    }

    ValidationException.check(
        configurationClass.isInstance(conf),
        "%s is not an instance of Configuration from the classloader for %s",
        conf,
        maybeConfigurable);

    DynMethods.BoundMethod setConf;
    try {
      setConf =
          DynMethods.builder("setConf")
              .impl(configurableInterface, configurationClass)
              .buildChecked()
              .bind(maybeConfigurable);
    } catch (NoSuchMethodException e) {
      // this shouldn't happen because Configurable was loaded and defines setConf
      throw new UnsupportedOperationException(
          "Failed to load Configuration.setConf after loading Configurable", e);
    }

    setConf.invoke(conf);
  }
}
