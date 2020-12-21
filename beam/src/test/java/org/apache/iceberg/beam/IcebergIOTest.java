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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

public class IcebergIOTest {
    private static final String stringSchema = "{\n" +
            "\t\"type\": \"record\",\n" +
            "\t\"name\": \"Word\",\n" +
            "\t\"fields\": [{\n" +
            "\t\t\"name\": \"word\",\n" +
            "\t\t\"type\": [\"null\", \"string\"],\n" +
            "\t\t\"default\": null\n" +
            "\t}]\n" +
            "}";

    private String getInputFile(String filename) {
        final URL resource = getClass().getClassLoader().getResource(filename);
        if (resource == null) {
            throw new IllegalArgumentException("file not found!");
        } else {
            try {
                return new File(resource.toURI()).getAbsolutePath();
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("file not found!");
            }
        }
    }

    public static class StringToGenericRecord extends DoFn<String, GenericRecord> {
        private final Schema schema;

        public StringToGenericRecord() {
            schema = new Schema.Parser().parse(stringSchema);
        }

        @ProcessElement
        public void processElement(@Element String word, OutputReceiver<GenericRecord> out) {
            GenericRecord record = new GenericData.Record(schema);
            record.put("word", word);
            out.output(record);
        }
    }

    @Test
    public void testWriteFiles() {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(options);
        String hiveMetastoreUrl = "thrift://localhost:9083/default";

        Schema schema = new Schema.Parser().parse(stringSchema);
        p.getCoderRegistry().registerCoderForClass(GenericRecord.class, AvroCoder.of(schema));

        PCollection<String> lines = p.apply(TextIO.read().from(getInputFile("words.txt"))).setCoder(StringUtf8Coder.of());

        PCollection<GenericRecord> records = lines.apply(ParDo.of(new StringToGenericRecord()));

        FileIO.Write<Void, GenericRecord> fileIO = FileIO.<GenericRecord>write()
                .via(AvroIO.sink(schema))
                .to("/tmp/fokko/")
                .withSuffix(".avro");

        WriteFilesResult<Void> output = records.apply(fileIO);
        org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(schema);
        TableIdentifier name = TableIdentifier.of("default", "test");

        IcebergIO.write(name, icebergSchema, hiveMetastoreUrl, output);

        p.run();
    }
}
