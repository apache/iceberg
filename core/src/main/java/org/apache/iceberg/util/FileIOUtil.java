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
package org.apache.iceberg.util;

import java.util.concurrent.ExecutorService;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileIOUtil {
  private static final Logger LOG = LoggerFactory.getLogger(FileIOUtil.class);

  public static BulkDeleter bulkDeleteManifests(FileIO io, Iterable<ManifestFile> files) {
    return bulkDelete(io, Iterables.transform(files, ManifestFile::path));
  }

  public static <C extends ContentFile<?>> BulkDeleter bulkDeleteFiles(
      FileIO io, Iterable<C> files) {
    return bulkDelete(io, Iterables.transform(files, file -> file.path().toString()));
  }

  public static BulkDeleter bulkDelete(FileIO io, Iterable<String> files) {
    return new BulkDeleter(io, files);
  }

  public static BulkDeleter bulkDelete(FileIO io, String file) {
    return new BulkDeleter(io, Sets.newHashSet(file));
  }

  public static class BulkDeleter {
    private final FileIO io;
    private final Iterable<String> files;
    private String name = "files";
    private ExecutorService service = null;

    private BulkDeleter(FileIO io, Iterable<String> files) {
      this.io = io;
      this.files = files;
    }

    public BulkDeleter name(String newName) {
      this.name = newName;
      return this;
    }

    public BulkDeleter executeWith(ExecutorService svc) {
      this.service = svc;
      return this;
    }

    public void execute() {
      if (io instanceof SupportsBulkOperations) {
        try {
          SupportsBulkOperations bulkIO = (SupportsBulkOperations) io;
          bulkIO.deleteFiles(files);
        } catch (BulkDeletionFailureException e) {
          LOG.warn("Failed to delete {} {}", e.numberFailedObjects(), name);
        } catch (Exception e) {
          // ignore
        }
      } else {
        Tasks.foreach(files)
            .noRetry()
            .executeWith(service)
            .suppressFailureWhenFinished()
            .onFailure((file, exc) -> LOG.warn("Delete failed for {}: {}", name, file, exc))
            .run(io::deleteFile);
      }
    }
  }
}
