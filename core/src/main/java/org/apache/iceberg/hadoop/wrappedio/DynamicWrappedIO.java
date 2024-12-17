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
package org.apache.iceberg.hadoop.wrappedio;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.common.DynMethods;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The wrapped IO methods in {@code WrappedIO}, dynamically loaded. Derived from {@code
 * org.apache.hadoop.io.wrappedio.impl.DynamicWrappedIO}.
 */
public final class DynamicWrappedIO {

  private static final Logger LOG = LoggerFactory.getLogger(DynamicWrappedIO.class);

  /** Classname of the wrapped IO class: {@value}. */
  public static final String WRAPPED_IO_CLASSNAME = "org.apache.hadoop.io.wrappedio.WrappedIO";

  /** Method name for bulk delete: {@value} */
  public static final String BULKDELETE_DELETE = "bulkDelete_delete";

  /** Method name for bulk delete: {@value} */
  public static final String BULKDELETE_PAGESIZE = "bulkDelete_pageSize";

  /**
   * Was wrapped IO loaded? In the hadoop codebase, this is true. But in other libraries it may not
   * always be true...this field is used to assist copy-and-paste adoption.
   */
  private final boolean loaded;

  /** Method binding. {@code WrappedIO.bulkDelete_delete(FileSystem, Path, Collection)}. */
  private final DynMethods.UnboundMethod bulkDeleteDeleteMethod;

  /** Method binding. {@code WrappedIO.bulkDelete_pageSize(FileSystem, Path)}. */
  private final DynMethods.UnboundMethod bulkDeletePageSizeMethod;

  /**
   * Dynamically load the WrappedIO class and its methods.
   *
   * @param loader classloader to use.
   */
  public DynamicWrappedIO(ClassLoader loader) {
    // load the class

    // Wrapped IO class.
    Class<?> wrappedIO = BindingUtils.loadClass(loader, WRAPPED_IO_CLASSNAME);

    loaded = wrappedIO != null;

    // bulk delete APIs
    bulkDeleteDeleteMethod =
        BindingUtils.loadStaticMethod(
            wrappedIO,
            List.class,
            BULKDELETE_DELETE,
            FileSystem.class,
            Path.class,
            Collection.class);

    bulkDeletePageSizeMethod =
        BindingUtils.loadStaticMethod(
            wrappedIO, Integer.class, BULKDELETE_PAGESIZE, FileSystem.class, Path.class);
  }

  /**
   * Is the wrapped IO class loaded?
   *
   * @return true if the wrappedIO class was found and loaded.
   */
  public boolean loaded() {
    return loaded;
  }

  /**
   * Are the bulk delete methods available?
   *
   * @return true if the methods were found.
   */
  public boolean bulkDeleteAvailable() {
    return !bulkDeleteDeleteMethod.isNoop();
  }

  /**
   * Get the maximum number of objects/files to delete in a single request.
   *
   * @param fileSystem filesystem
   * @param path path to delete under.
   * @return a number greater than or equal to zero.
   * @throws UnsupportedOperationException bulk delete under that path is not supported.
   * @throws IllegalArgumentException path not valid.
   * @throws RuntimeException invocation failure.
   */
  public int bulkDelete_pageSize(final FileSystem fileSystem, final Path path) {
    BindingUtils.checkAvailable(bulkDeletePageSizeMethod);
    return bulkDeletePageSizeMethod.invoke(null, fileSystem, path);
  }

  /**
   * Delete a list of files/objects.
   *
   * <ul>
   *   <li>Files must be under the path provided in {@code base}.
   *   <li>The size of the list must be equal to or less than the page size.
   *   <li>Directories are not supported; the outcome of attempting to delete directories is
   *       undefined (ignored; undetected, listed as failures...).
   *   <li>The operation is not atomic.
   *   <li>The operation is treated as idempotent: network failures may trigger resubmission of the
   *       request -any new objects created under a path in the list may then be deleted.
   *   <li>There is no guarantee that any parent directories exist after this call.
   * </ul>
   *
   * @param fs filesystem
   * @param base path to delete under.
   * @param paths list of paths which must be absolute and under the base path.
   * @return a list of all the paths which couldn't be deleted for a reason other than "not found"
   *     and any associated error message.
   * @throws UnsupportedOperationException bulk delete under that path is not supported.
   * @throws IllegalArgumentException if a path argument is invalid.
   * @throws RuntimeException for any failure.
   */
  public List<Map.Entry<Path, String>> bulkDelete_delete(
      FileSystem fs, Path base, Collection<Path> paths) {
    BindingUtils.checkAvailable(bulkDeleteDeleteMethod);
    return bulkDeleteDeleteMethod.invoke(null, fs, base, paths);
  }
}
