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

package org.apache.iceberg.beam;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;

public class StringToGenericRecord extends DoFn<String, GenericRecord> {
    private final Schema schema;

    public StringToGenericRecord(String schema) {
        this.schema = new Schema.Parser().parse(schema);
    }

    @ProcessElement
    public void processElement(@Element String word, OutputReceiver<GenericRecord> out) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("word", word);
        out.output(record);
    }
}
