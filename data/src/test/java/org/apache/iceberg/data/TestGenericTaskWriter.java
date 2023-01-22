/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestGenericTaskWriter extends TableTestBase {
    private FileFormat fileFormat;
    private final long TARGET_FILE_SIZE_IN_BYTES = 50L * 1024 * 1024;

    public TestGenericTaskWriter(String fileFormat) {
        super(2);
        this.fileFormat = FileFormat.fromString(fileFormat);
    }

    @Parameterized.Parameters(name = "FileFormat = {0}")
    public static Object[][] parameters() {
        return new Object[][]{{"avro"}, {"orc"}, {"parquet"}};
    }

    public List<Record> getTestRecords() {
        GenericRecord genericRecord = GenericRecord.create(table.schema());
        List<Record> records = Lists.newArrayList();
        for (int i = 0; i < 50; i++) {
            GenericRecord record = genericRecord.copy();
            record.setField("id",  i);
            record.setField("data", i % 2 == 0 ? "aaa" : "bbb");
            records.add(record);
        }
        return records;
    }

    @Test
    public void writeDataTest() throws IOException{
        GenericAppenderFactory appenderFactory = new GenericAppenderFactory(table.schema(), table.spec());
        OutputFileFactory outputFileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();
        GenericTaskWriter<Record> genericTaskWriter = new GenericTaskWriter(table.spec(), fileFormat, appenderFactory, outputFileFactory, table.io(), TARGET_FILE_SIZE_IN_BYTES);
        List<Record> testRecords = getTestRecords();
        for (Record record : testRecords) {
            genericTaskWriter.write(record);
        }

        Assert.assertEquals("GenericTaskWriter auto partition have 2 data files", 2, genericTaskWriter.dataFiles().length);

        AppendFiles newAppend = table.newAppend();
        for (DataFile dataFile : genericTaskWriter.dataFiles()) {
            newAppend.appendFile(dataFile);
        }
        newAppend.commit();

        IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(table);
        ArrayList<Record> actualRecords = Lists.newArrayList(scanBuilder.build().iterator());
        StructLikeSet actualSet = StructLikeSet.create(table.schema().asStruct());
        actualSet.addAll(actualRecords);

        StructLikeSet expectedSet = StructLikeSet.create(table.schema().asStruct());
        expectedSet.addAll(testRecords);

        Assert.assertEquals("should have same records", expectedSet, actualSet);
    }

}
