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

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class FilenameToDataFile extends DoFn<String, WrittenDataFile> {

    public FilenameToDataFile() {
    }

    @ProcessElement
    public void processElement(@Element String filename, OutputReceiver<WrittenDataFile> out) {
        long filesize = -1;
        int records = 0;
        try {
            MatchResult vo = FileSystems.match(filename);
            try (ReadableByteChannel channel = FileSystems.open(FileSystems.matchNewResource(filename, false));
                 InputStream in = Channels.newInputStream(channel);
                 DataFileStream<GenericRecord> reader = new DataFileStream<>(in, new GenericDatumReader<>())) {

                while (reader.hasNext()) {
                    ++records;
                    reader.next();
                }
            }

            filesize = vo.metadata().get(0).sizeBytes();
        } catch (IOException e) {
            e.printStackTrace();
        }

        out.output(new WrittenDataFile(
                filename,
                filesize,
                records
        ));
    }
}
