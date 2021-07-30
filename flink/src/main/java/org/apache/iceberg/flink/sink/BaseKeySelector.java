/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.flink.sink;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.RowDataWrapper;


abstract class BaseKeySelector<IN, KEY> implements KeySelector<IN, KEY> {

  protected final Schema schema;
  protected final RowType flinkSchema;
  protected transient RowDataWrapper rowDataWrapper;

  protected BaseKeySelector(Schema schema, RowType flinkSchema) {
    this.schema = schema;
    this.flinkSchema = flinkSchema;
  }

  /**
   * Construct the {@link RowDataWrapper} lazily here because few members in it are not serializable. In this way, we
   * don't have to serialize them with forcing.
   */
  protected RowDataWrapper lazyRowDataWrapper() {
    if (rowDataWrapper == null) {
      rowDataWrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
    }
    return rowDataWrapper;
  }
}
