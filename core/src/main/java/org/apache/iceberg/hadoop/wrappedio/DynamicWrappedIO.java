/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.hadoop.wrappedio;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The wrapped IO methods in {@code WrappedIO}, dynamically loaded.
 */
public final class DynamicWrappedIO {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopFileIO.class);

  /**
   * Classname of the wrapped IO class: {@value}.
   */
  public static final String WRAPPED_IO_CLASSNAME =
      "org.apache.hadoop.io.wrappedio.WrappedIO";

  /**
   * Method name for bulk delete: {@value}
   */
  public static final String BULKDELETE_DELETE = "bulkDelete_delete";

  /**
   * Method name for bulk delete: {@value}
   */
  public static final String BULKDELETE_PAGESIZE = "bulkDelete_pageSize";

  /**
   * Wrapped IO class.
   */
  private final Class<?> wrappedIO;

  /**
   * Was wrapped IO loaded?
   * In the hadoop codebase, this is true.
   * But in other libraries it may not always be true...this
   * field is used to assist copy-and-paste adoption.
   */
  private final boolean loaded;

  /**
   * Method binding.
   * {@code WrappedIO.bulkDelete_delete(FileSystem, Path, Collection)}.
   */
  private final DynMethods.UnboundMethod bulkDeleteDeleteMethod;

  /**
   * Method binding.
   * {@code WrappedIO.bulkDelete_pageSize(FileSystem, Path)}.
   */
  private final DynMethods.UnboundMethod bulkDeletePageSizeMethod;

  /**
   * Dynamically load the WrappedIO class and its methods.
   * @param loader classloader to use.
   */
  public DynamicWrappedIO(ClassLoader loader) {
    // load the class
    final DynClasses.Builder builder = DynClasses.builder();
    wrappedIO = builder
        .loader(loader)
        .impl(WRAPPED_IO_CLASSNAME)
        .orNull()
        .build();

    loaded = wrappedIO != null;
    if (loaded) {

      // bulk delete APIs
      bulkDeleteDeleteMethod = loadInvocation(
          wrappedIO,
          List.class,
          BULKDELETE_DELETE,
          FileSystem.class,
          Path.class,
          Collection.class);

      bulkDeletePageSizeMethod = loadInvocation(
          wrappedIO,
          Integer.class,
          BULKDELETE_PAGESIZE,
          FileSystem.class,
          Path.class);

    } else {
      // set to no-ops.
      // the loadInvocation call would do this anyway;
      // this just makes the outcome explicit.
      bulkDeleteDeleteMethod = noop(BULKDELETE_DELETE);
      bulkDeletePageSizeMethod = noop(BULKDELETE_PAGESIZE);
    }

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
   *
   * @return a number greater than or equal to zero.
   *
   * @throws UnsupportedOperationException bulk delete under that path is not supported.
   * @throws IllegalArgumentException path not valid.
   * @throws RuntimeException invocation failure.
   */
  public int bulkDelete_pageSize(final FileSystem fileSystem, final Path path) {
    return bulkDeletePageSizeMethod.invoke(null, fileSystem, path);
  }

  /**
   * Delete a list of files/objects.
   * <ul>
   *   <li>Files must be under the path provided in {@code base}.</li>
   *   <li>The size of the list must be equal to or less than the page size.</li>
   *   <li>Directories are not supported; the outcome of attempting to delete
   *       directories is undefined (ignored; undetected, listed as failures...).</li>
   *   <li>The operation is not atomic.</li>
   *   <li>The operation is treated as idempotent: network failures may
   *        trigger resubmission of the request -any new objects created under a
   *        path in the list may then be deleted.</li>
   *    <li>There is no guarantee that any parent directories exist after this call.
   *    </li>
   * </ul>
   *
   * @param fs filesystem
   * @param base path to delete under.
   * @param paths list of paths which must be absolute and under the base path.
   *
   * @return a list of all the paths which couldn't be deleted for a reason other than
   *          "not found" and any associated error message.
   *
   * @throws UnsupportedOperationException bulk delete under that path is not supported.
   * @throws IllegalArgumentException if a path argument is invalid.
   * @throws RuntimeException for any failure.
   */
  public List<Map.Entry<Path, String>> bulkDelete_delete(FileSystem fs,
      Path base,
      Collection<Path> paths) {

    return bulkDeleteDeleteMethod.invoke(null, fs, base, paths);
  }

  /**
   * Get an invocation from the source class, which will be unavailable() if
   * the class is null or the method isn't found.
   *
   * @param <T> return type
   * @param source source. If null, the method is a no-op.
   * @param returnType return type class (unused)
   * @param name method name
   * @param parameterTypes parameters
   *
   * @return the method or "unavailable"
   */
  private static <T> DynMethods.UnboundMethod loadInvocation(
      Class<?> source, Class<? extends T> returnType, String name, Class<?>... parameterTypes) {

    if (source != null) {
      final DynMethods.UnboundMethod m = new DynMethods.Builder(name)
          .impl(source, name, parameterTypes)
          .orNoop()
          .build();
      if (m.isNoop()) {
        // this is a sign of a mismatch between this class's expected
        // signatures and actual ones.
        // log at debug.
        LOG.debug("Failed to load method {} from {}", name, source);
      } else {
        LOG.debug("Found method {} from {}", name, source);
      }
      return m;
    } else {
      return noop(name);
    }
  }

  /**
   * Create a no-op method.
   *
   * @param name method name
   *
   * @return a no-op method.
   */
  static DynMethods.UnboundMethod noop(final String name) {
    return new DynMethods.Builder(name).orNoop().build();
  }

}
