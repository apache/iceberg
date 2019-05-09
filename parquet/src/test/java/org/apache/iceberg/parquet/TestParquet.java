package org.apache.iceberg.parquet;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.Files.localOutput;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestParquet {

    @Rule
    public TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void testRowGroupSizeConfigurable() throws IOException {
        // Without an explicit writer function
        File parquetFile = generateFileWithTwoRowGroups(null);

        try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(localInput(parquetFile)))) {
            Assert.assertEquals(2, reader.getRowGroups().size());
        }
    }

    @Test
    public void testRowGroupSizeConfigurableWithWriter() throws IOException {
        File parquetFile = generateFileWithTwoRowGroups(ParquetAvroWriter::buildWriter);

        try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(localInput(parquetFile)))) {
            Assert.assertEquals(2, reader.getRowGroups().size());
        }
    }

    static File writeRecords(TemporaryFolder temporaryFolder, Schema schema, Map<String, String> properties,
                             Function<MessageType, ParquetValueWriter<?>> createWriterFunc,
                             GenericData.Record... records) throws IOException {
        File tmpFolder = temporaryFolder.newFolder("parquet");
        String filename = UUID.randomUUID().toString();
        File file = new File(tmpFolder, FileFormat.PARQUET.addExtension(filename));
        try (FileAppender<GenericData.Record> writer = Parquet.write(localOutput(file))
                .schema(schema)
                .setAll(properties)
                .createWriterFunc(createWriterFunc)
                .build()) {
            writer.addAll(Lists.newArrayList(records));
        }
        return file;
    }

    private File writeRecords(Schema schema, Map<String, String> properties,
                              Function<MessageType, ParquetValueWriter<?>> createWriterFunc,
                              GenericData.Record... records) throws IOException {
        return writeRecords(temp, schema, properties, createWriterFunc, records);
    }

    private File generateFileWithTwoRowGroups(Function<MessageType, ParquetValueWriter<?>> createWriterFunc)
            throws IOException {
        Schema schema = new Schema(
                optional(1, "intCol", IntegerType.get())
        );

        int minimumRowGroupRecordCount = 100;
        int desiredRecordCount = minimumRowGroupRecordCount + 1;

        List<GenericData.Record> records = new ArrayList<>(desiredRecordCount);
        org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());
        for (int i = 1; i <= desiredRecordCount; i++) {
            GenericData.Record record = new GenericData.Record(avroSchema);
            record.put("intCol", i);
            records.add(record);
        }

        // Force multiple row groups by making the byte size very small
        // Note there'a also minimumRowGroupRecordCount which cannot be configured so we have to write
        // at least that many records for a new row group to occur
        return writeRecords(schema,
                ImmutableMap.of(PARQUET_ROW_GROUP_SIZE_BYTES,
                        Integer.toString(minimumRowGroupRecordCount * Integer.BYTES)),
                createWriterFunc,
                records.toArray(new GenericData.Record[]{}));
    }
}
