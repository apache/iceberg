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

import java.io.File;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;

public class PathUtil {

  private PathUtil() {}

  public static String getRelativePath(String tableLocation, String fileLocation) {
    URI tableUri = URI.create(tableLocation);
    URI fileUri = URI.create(fileLocation);
    String tablePath = tableUri.isAbsolute() ? tableUri.getRawPath() : new File(tableUri.getPath()).getAbsolutePath();
    String filePath = fileUri.isAbsolute() ? fileUri.getRawPath() : new File(fileUri.getPath()).getAbsolutePath();
    Path sourceFile = Paths.get(tablePath);
    Path targetFile = Paths.get(filePath);
    return sourceFile.relativize(targetFile).toString();
  }

  public static String getAbsolutePath(String basePath, String relativePath) {
    Path base = FileSystems.getDefault().getPath(basePath);
    Path resolvedPath = base.resolve(relativePath);
    return resolvedPath.normalize().toString();
  }
}
