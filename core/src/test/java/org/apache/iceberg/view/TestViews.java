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
package org.apache.iceberg.view;

import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.types.Types;

public class TestViews {

  public static TestView createSampleTestView(String name) {
    return new TestView(
        new TestViewOperations(/* version= */ 1, /* timestampMillis= */ 1737436503101L), name);
  }

  public static class TestView extends BaseView {

    public TestView(ViewOperations ops, String name) {
      super(ops, name);
    }
  }

  public static class TestViewOperations implements ViewOperations {

    private ViewMetadata metadata;

    public TestViewOperations(int version, long timestampMillis) {
      ViewMetadata.Builder builder = ViewMetadata.builder();
      builder.addSchema(new Schema(Types.NestedField.required(1, "foo", Types.IntegerType.get())));
      builder.addVersion(
          ImmutableViewVersion.builder()
              .versionId(version)
              .schemaId(0)
              .timestampMillis(timestampMillis)
              .defaultNamespace(Namespace.of("foo.bar"))
              .build());
      builder.setCurrentVersionId(version).setLocation("s3://foo/bar");
      this.metadata = builder.build();
    }

    @Override
    public ViewMetadata current() {
      return metadata;
    }

    @Override
    public ViewMetadata refresh() {
      return metadata;
    }

    @Override
    public void commit(ViewMetadata base, ViewMetadata updated) {}
  }

  private TestViews() {}
}
