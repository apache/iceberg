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

import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;

public class ScanTaskUtil {

  private ScanTaskUtil() {}

  public static long contentSizeInBytes(ContentFile<?> file) {
    if (file.content() == FileContent.DATA) {
      return file.fileSizeInBytes();
    } else {
      DeleteFile deleteFile = (DeleteFile) file;
      return isDV(deleteFile) ? deleteFile.contentSizeInBytes() : deleteFile.fileSizeInBytes();
    }
  }

  public static long contentSizeInBytes(Iterable<? extends ContentFile<?>> files) {
    long size = 0L;
    for (ContentFile<?> file : files) {
      size += contentSizeInBytes(file);
    }
    return size;
  }

  private static boolean isDV(DeleteFile deleteFile) {
    return deleteFile.format() == FileFormat.PUFFIN;
  }
}
