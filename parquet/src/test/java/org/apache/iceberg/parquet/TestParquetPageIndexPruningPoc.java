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
package org.apache.iceberg.parquet;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestParquetPageIndexPruningPoc {
  private static final Logger LOG = LoggerFactory.getLogger(TestParquetPageIndexPruningPoc.class);
  private static final int RECORD_COUNT = 100_000;

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "group_id", Types.IntegerType.get()),
          Types.NestedField.optional(3, "payload", Types.StringType.get()));

  private static final Schema PAYLOAD_ONLY_SCHEMA = new Schema(SCHEMA.findField("payload"));

  private static final Schema FILTER_AND_PAYLOAD_SCHEMA =
      new Schema(SCHEMA.findField("id"), SCHEMA.findField("payload"));

  private static final Schema SCHEMA_WITH_POS =
      TypeUtil.join(SCHEMA, new Schema(MetadataColumns.ROW_POSITION));

  private static final Schema SCHEMA_WITH_POS_AND_ROW_ID =
      TypeUtil.join(SCHEMA, new Schema(MetadataColumns.ROW_POSITION, MetadataColumns.ROW_ID));

  private static final long BASE_ROW_ID = 1_000_000L;

  @TempDir private File tempDir;

  @Test
  public void testParquetJavaCanPrunePages() throws IOException {
    File parquetFile = new File(tempDir, "page-index.parquet");
    writeSortedFile(parquetFile);

    InputFile inputFile = Files.localInput(parquetFile);

    FilterPredicate predicate = and(gtEq(longColumn("id"), 50_000L), lt(longColumn("id"), 50_100L));

    FilterCompat.Filter filter = FilterCompat.get(predicate);

    // Disable other pruning mechanisms so this POC isolates ColumnIndex pruning.
    ParquetReadOptions options =
        ParquetReadOptions.builder()
            .useStatsFilter(false)
            .useDictionaryFilter(false)
            .useBloomFilter(false)
            .useRecordFilter(false)
            .useColumnIndexFilter(true)
            .withRecordFilter(filter)
            .build();

    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inputFile), options)) {

      assertThat(reader.getRowGroups()).hasSize(1);

      BlockMetaData rowGroup = reader.getRowGroups().get(0);
      assertThat(rowGroup.getRowCount()).isEqualTo(RECORD_COUNT);

      ColumnChunkMetaData idColumn =
          rowGroup.getColumns().stream()
              .filter(column -> column.getPath().toDotString().equals("id"))
              .findFirst()
              .orElseThrow(() -> new IllegalStateException("Cannot find id column"));

      ColumnIndex columnIndex = reader.readColumnIndex(idColumn);
      OffsetIndex offsetIndex = reader.readOffsetIndex(idColumn);

      assertThat(columnIndex).isNotNull();
      assertThat(offsetIndex).isNotNull();
      assertThat(offsetIndex.getPageCount()).isGreaterThan(1);

      long filteredRecordCount = reader.getFilteredRecordCount();

      assertThat(filteredRecordCount).isGreaterThanOrEqualTo(100L).isLessThan(RECORD_COUNT);

      PageReadStore pages = reader.readFilteredRowGroup(0);

      assertThat(pages).isNotNull();
      assertThat(pages.getRowCount()).isEqualTo(filteredRecordCount);

      PrimitiveIterator.OfLong rowIndexes =
          pages
              .getRowIndexes()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Filtered PageReadStore did not expose row indexes"));

      long count = 0L;
      long firstRowIndex = -1L;
      long lastRowIndex = -1L;

      while (rowIndexes.hasNext()) {
        long rowIndex = rowIndexes.nextLong();

        if (count == 0L) {
          firstRowIndex = rowIndex;
        }

        lastRowIndex = rowIndex;
        count += 1L;
      }

      assertThat(count).isEqualTo(filteredRecordCount);

      // Page pruning is conservative. It may include extra rows from candidate pages,
      // but it must cover every exact match in [50_000, 50_100).
      assertThat(firstRowIndex).isLessThanOrEqualTo(50_000L);
      assertThat(lastRowIndex).isGreaterThanOrEqualTo(50_099L);

      LOG.info(
          "rowGroups={}, pages={}, fullRows={}, candidateRows={}, "
              + "firstRowIndex={}, lastRowIndex={}\n",
          reader.getRowGroups().size(),
          offsetIndex.getPageCount(),
          RECORD_COUNT,
          filteredRecordCount,
          firstRowIndex,
          lastRowIndex);
    }
  }

  @Test
  public void testParquetJavaCanEliminateAllPages() throws IOException {
    File parquetFile = new File(tempDir, "page-index-no-match.parquet");
    writeSortedFile(parquetFile);

    InputFile inputFile = Files.localInput(parquetFile);

    // The row-group range is [0, 100000), so this predicate cannot match any page.
    FilterPredicate predicate =
        and(gtEq(longColumn("id"), 200_000L), lt(longColumn("id"), 200_100L));

    ParquetReadOptions options =
        ParquetReadOptions.builder()
            .useStatsFilter(false)
            .useDictionaryFilter(false)
            .useBloomFilter(false)
            .useRecordFilter(false)
            .useColumnIndexFilter(true)
            .withRecordFilter(FilterCompat.get(predicate))
            .build();

    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inputFile), options)) {

      assertThat(reader.getFilteredRecordCount()).isZero();
      assertThat(reader.readFilteredRowGroup(0)).isNull();
    }
  }

  @Test
  public void testPageIndexWithFilterColumnInRequestedSchema() throws IOException {

    File parquetFile = new File(tempDir, "page-index-filter-in-projection.parquet");

    writeSortedFile(parquetFile);

    InputFile inputFile = Files.localInput(parquetFile);

    FilterPredicate predicate = and(gtEq(longColumn("id"), 50_000L), lt(longColumn("id"), 50_100L));

    ParquetReadOptions options =
        ParquetReadOptions.builder()
            .useStatsFilter(false)
            .useDictionaryFilter(false)
            .useBloomFilter(false)
            .useRecordFilter(false)
            .useColumnIndexFilter(true)
            .withRecordFilter(FilterCompat.get(predicate))
            .build();

    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inputFile), options)) {

      MessageType fileSchema = reader.getFileMetaData().getSchema();

      MessageType requestedSchema =
          ParquetSchemaUtil.pruneColumns(fileSchema, FILTER_AND_PAYLOAD_SCHEMA);

      assertThat(requestedSchema.getColumns())
          .extracting(column -> column.getPath()[0])
          .containsExactlyInAnyOrder("id", "payload");

      reader.setRequestedSchema(requestedSchema);

      long candidateRows = reader.getFilteredRecordCount();

      assertThat(candidateRows).isGreaterThanOrEqualTo(100L).isLessThan(RECORD_COUNT);

      PageReadStore pages = reader.readFilteredRowGroup(0);

      assertThat(pages).isNotNull();
      assertThat(pages.getRowCount()).isEqualTo(candidateRows);

      LOG.info(
          "Spark-like path: requestedColumns={}, candidateRows={}",
          requestedSchema.getColumns(),
          candidateRows);
    }
  }

  @Test
  public void testFilterOutsideProjectionRequiresPlanningFirst() throws IOException {

    File parquetFile = new File(tempDir, "page-index-filter-outside-projection.parquet");

    writeSortedFile(parquetFile);

    InputFile inputFile = Files.localInput(parquetFile);

    FilterPredicate predicate = and(gtEq(longColumn("id"), 50_000L), lt(longColumn("id"), 50_100L));

    ParquetReadOptions options =
        ParquetReadOptions.builder()
            .useStatsFilter(false)
            .useDictionaryFilter(false)
            .useBloomFilter(false)
            .useRecordFilter(false)
            .useColumnIndexFilter(true)
            .withRecordFilter(FilterCompat.get(predicate))
            .build();

    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inputFile), options)) {

      MessageType fileSchema = reader.getFileMetaData().getSchema();

      MessageType payloadOnlySchema =
          ParquetSchemaUtil.pruneColumns(fileSchema, PAYLOAD_ONLY_SCHEMA);

      /*
       * Plan and cache RowRanges while the filter column "id"
       * is still available in the reader's internal path set.
       */
      long candidateRows = reader.getFilteredRecordCount();

      assertThat(candidateRows).isGreaterThanOrEqualTo(100L).isLessThan(RECORD_COUNT);

      /*
       * Narrow the columns that will actually be loaded only after
       * the page-index RowRanges have been planned.
       */
      reader.setRequestedSchema(payloadOnlySchema);

      PageReadStore pages = reader.readFilteredRowGroup(0);

      assertThat(pages).isNotNull();
      assertThat(pages.getRowCount()).isEqualTo(candidateRows);

      assertThat(
              pages.getPageReader(payloadOnlySchema.getColumnDescription(new String[] {"payload"})))
          .isNotNull();

      LOG.info(
          "Filter outside projection works only when RowRanges "
              + "are planned before projection: candidateRows={}",
          candidateRows);
    }
  }

  @Test
  public void testPageIndexProducesDisjointRowRanges() throws IOException {

    File parquetFile = new File(tempDir, "page-index-disjoint-ranges.parquet");

    writeSortedFile(parquetFile);

    InputFile inputFile = Files.localInput(parquetFile);

    FilterPredicate firstRange =
        and(gtEq(longColumn("id"), 10_000L), lt(longColumn("id"), 10_100L));

    FilterPredicate secondRange =
        and(gtEq(longColumn("id"), 50_000L), lt(longColumn("id"), 50_100L));

    FilterPredicate predicate = or(firstRange, secondRange);

    ParquetReadOptions options =
        ParquetReadOptions.builder()
            .useStatsFilter(false)
            .useDictionaryFilter(false)
            .useBloomFilter(false)
            .useRecordFilter(false)
            .useColumnIndexFilter(true)
            .withRecordFilter(FilterCompat.get(predicate))
            .build();

    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inputFile), options)) {

      long candidateRows = reader.getFilteredRecordCount();
      LOG.info("candidateRows={}", candidateRows);

      assertThat(candidateRows).isGreaterThanOrEqualTo(200L).isLessThan(RECORD_COUNT);

      PageReadStore pages = reader.readFilteredRowGroup(0);

      assertThat(pages).isNotNull();
      assertThat(pages.getRowCount()).isEqualTo(candidateRows);

      PrimitiveIterator.OfLong rowIndexes =
          pages
              .getRowIndexes()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Filtered PageReadStore did not expose row indexes"));

      long count = 0L;
      long firstRowIndex = -1L;
      long lastRowIndex = -1L;

      long previousRowIndex = -1L;
      long firstGapStart = -1L;
      long firstGapEnd = -1L;
      boolean sawGap = false;

      while (rowIndexes.hasNext()) {
        long rowIndex = rowIndexes.nextLong();

        if (count == 0L) {
          firstRowIndex = rowIndex;
        }

        if (rowIndex == 10999) {
          LOG.info("Saw rowIndex=999, which is a known gap in the filter predicate");
        }

        if (previousRowIndex >= 0L && rowIndex > previousRowIndex + 1L) {

          if (!sawGap) {
            firstGapStart = previousRowIndex + 1L;
            firstGapEnd = rowIndex - 1L;
          }

          sawGap = true;
        }

        previousRowIndex = rowIndex;
        lastRowIndex = rowIndex;
        count += 1L;
      }

      assertThat(count).isEqualTo(candidateRows);
      assertThat(sawGap).isTrue();

      assertThat(firstRowIndex).isLessThanOrEqualTo(10_000L);
      assertThat(lastRowIndex).isGreaterThanOrEqualTo(50_099L);

      assertThat(firstGapStart).isPositive();
      assertThat(firstGapEnd).isGreaterThan(firstGapStart);

      LOG.info(
          "candidateRows={}, firstRowIndex={}, lastRowIndex={}, " + "firstGap=[{},{}]",
          candidateRows,
          firstRowIndex,
          lastRowIndex,
          firstGapStart,
          firstGapEnd);
    }
  }

  @Test
  public void testRowIndexCoordinateSystemAcrossRowGroups() throws IOException {
    File parquetFile = new File(tempDir, "page-index-multiple-row-groups.parquet");
    writeMultiRowGroupFile(parquetFile);
    InputFile inputFile = Files.localInput(parquetFile);
    long secondRowGroupStart;
    long secondRowGroupCount;

    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(inputFile), ParquetReadOptions.builder().build())) {
      assertThat(reader.getRowGroups().size())
          .as("The test file must contain multiple row groups")
          .isGreaterThan(1);
      BlockMetaData firstRowGroup = reader.getRowGroups().get(0);
      BlockMetaData secondRowGroup = reader.getRowGroups().get(1);
      secondRowGroupStart = firstRowGroup.getRowCount();
      secondRowGroupCount = secondRowGroup.getRowCount();
      assertThat(secondRowGroupStart)
          .as("The second row group must start after file row 0")
          .isPositive();
      assertThat(secondRowGroupCount)
          .as("The second row group must be large enough for a selective range")
          .isGreaterThan(200L);
      LOG.info(
          "rowGroupCount={}, firstRowGroupRows={}, "
              + "secondRowGroupStart={}, secondRowGroupRows={}",
          reader.getRowGroups().size(),
          firstRowGroup.getRowCount(),
          secondRowGroupStart,
          secondRowGroupCount);
    }

    long targetStart = secondRowGroupStart + Math.min(1_000L, secondRowGroupCount / 2L);
    long targetEnd = Math.min(targetStart + 100L, secondRowGroupStart + secondRowGroupCount);
    FilterPredicate predicate =
        and(gtEq(longColumn("id"), targetStart), lt(longColumn("id"), targetEnd));
    ParquetReadOptions options =
        ParquetReadOptions.builder()
            .useStatsFilter(false)
            .useDictionaryFilter(false)
            .useBloomFilter(false)
            .useRecordFilter(false)
            .useColumnIndexFilter(true)
            .withRecordFilter(FilterCompat.get(predicate))
            .build();

    try (ParquetFileReader reader = ParquetFileReader.open(ParquetIO.file(inputFile), options)) {
      PageReadStore pages = reader.readFilteredRowGroup(1);
      assertThat(pages).as("The target range must produce a filtered PageReadStore").isNotNull();
      assertThat(pages.getRowCount())
          .as("Page pruning is conservative but must reduce the row group")
          .isGreaterThanOrEqualTo(targetEnd - targetStart)
          .isLessThan(secondRowGroupCount);
      long rowIndexOffset =
          pages
              .getRowIndexOffset()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Filtered PageReadStore did not expose a row-index offset"));
      /*
       * parquet-java 1.17.1 returns row-group-relative indexes from
       * getRowIndexes(), while getRowIndexOffset() identifies the row group's
       * file-level starting position.
       */
      assertThat(rowIndexOffset)
          .as("Row-index offset must equal the second row group's file position")
          .isEqualTo(secondRowGroupStart);
      PrimitiveIterator.OfLong rowIndexes =
          pages
              .getRowIndexes()
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Filtered PageReadStore did not expose row indexes"));

      long candidateRowCount = 0L;
      long firstRelativeRowIndex = -1L;
      long lastRelativeRowIndex = -1L;
      while (rowIndexes.hasNext()) {
        long relativeRowIndex = rowIndexes.nextLong();
        if (candidateRowCount == 0L) {
          firstRelativeRowIndex = relativeRowIndex;
        }
        lastRelativeRowIndex = relativeRowIndex;
        candidateRowCount += 1L;
      }
      assertThat(candidateRowCount)
          .as("The row-index iterator must cover every candidate row")
          .isEqualTo(pages.getRowCount());
      assertThat(firstRelativeRowIndex).isNotNegative();
      assertThat(lastRelativeRowIndex)
          .isGreaterThanOrEqualTo(firstRelativeRowIndex)
          .isLessThan(secondRowGroupCount);
      long relativeTargetStart = targetStart - secondRowGroupStart;
      long relativeTargetEnd = targetEnd - secondRowGroupStart;
      /*
       * The returned row indexes are relative to the second row group.
       * The candidate page may begin before and end after the exact predicate
       * range, but it must cover the complete range.
       */
      assertThat(firstRelativeRowIndex)
          .as("The first candidate row must begin at or before the target")
          .isLessThanOrEqualTo(relativeTargetStart);
      assertThat(lastRelativeRowIndex)
          .as("The last candidate row must end at or after the target")
          .isGreaterThanOrEqualTo(relativeTargetEnd - 1L);

      long firstAbsoluteRowIndex = rowIndexOffset + firstRelativeRowIndex;
      long lastAbsoluteRowIndex = rowIndexOffset + lastRelativeRowIndex;
      assertThat(firstAbsoluteRowIndex)
          .as("The absolute candidate range must begin at or before the target")
          .isLessThanOrEqualTo(targetStart);
      assertThat(lastAbsoluteRowIndex)
          .as("The absolute candidate range must end at or after the target")
          .isGreaterThanOrEqualTo(targetEnd - 1L);
      assertThat(firstAbsoluteRowIndex)
          .as("Candidate positions must belong to the second row group")
          .isGreaterThanOrEqualTo(secondRowGroupStart);
      assertThat(lastAbsoluteRowIndex)
          .as("Candidate positions must not exceed the second row group")
          .isLessThan(secondRowGroupStart + secondRowGroupCount);
      LOG.info(
          "secondRowGroupStart={}, rowIndexOffset={}, "
              + "target=[{},{}), candidateRows={}, "
              + "relativeRowIndexes=[{},{}], absoluteRowIndexes=[{},{}]",
          secondRowGroupStart,
          rowIndexOffset,
          targetStart,
          targetEnd,
          candidateRowCount,
          firstRelativeRowIndex,
          lastRelativeRowIndex,
          firstAbsoluteRowIndex,
          lastAbsoluteRowIndex);
    }
  }

  @Test
  public void testIcebergRowReaderUsesPageIndex() throws IOException {
    File parquetFile = new File(tempDir, "iceberg-page-index-reader.parquet");

    writeSortedFile(parquetFile);

    InputFile inputFile = Files.localInput(parquetFile);

    Expression filter =
        Expressions.and(
            Expressions.greaterThanOrEqual("id", 50_000L), Expressions.lessThan("id", 50_100L));

    long baselineCount = 0L;

    try (CloseableIterable<Record> rows =
        Parquet.read(inputFile)
            .project(SCHEMA)
            .filter(filter)
            .createReaderFunc(GenericParquetReaders::buildReader)
            .build()) {

      for (Record ignored : rows) {
        baselineCount += 1L;
      }
    }

    assertThat(baselineCount).isEqualTo(RECORD_COUNT);

    long candidateCount = 0L;
    long minId = Long.MAX_VALUE;
    long maxId = Long.MIN_VALUE;

    try (CloseableIterable<Record> rows =
        Parquet.read(inputFile)
            .project(SCHEMA)
            .filter(filter)
            .createReaderFunc(GenericParquetReaders::buildReader)
            .enablePageIndexFilteringForPoc()
            .build()) {

      for (Record row : rows) {
        long id = (Long) row.getField("id");

        candidateCount += 1L;
        minId = Math.min(minId, id);
        maxId = Math.max(maxId, id);
      }
    }

    assertThat(candidateCount).isGreaterThanOrEqualTo(100L).isLessThan(RECORD_COUNT);

    assertThat(minId).isLessThanOrEqualTo(50_000L);
    assertThat(maxId).isGreaterThanOrEqualTo(50_099L);

    LOG.info(
        "Iceberg reader POC: baselineRows={}, "
            + "pageIndexCandidateRows={}, candidateIdRange=[{},{}]",
        baselineCount,
        candidateCount,
        minId,
        maxId);
  }

  @Test
  public void testIcebergReaderPreservesPositionsForDisjointPageRanges() throws IOException {
    File parquetFile = new File(tempDir, "page-index-position-disjoint.parquet");
    writeSortedFile(parquetFile);
    InputFile inputFile = Files.localInput(parquetFile);

    Expression firstRange =
        Expressions.and(
            Expressions.greaterThanOrEqual("id", 10_000L), Expressions.lessThan("id", 10_100L));
    Expression secondRange =
        Expressions.and(
            Expressions.greaterThanOrEqual("id", 50_000L), Expressions.lessThan("id", 50_100L));
    Expression filter = Expressions.or(firstRange, secondRange);

    long candidateCount = 0L;
    long previousPosition = -1L;
    boolean sawGap = false;

    boolean sawFirstTarget = false;
    boolean sawSecondTarget = false;

    long firstPosition = -1L;
    long lastPosition = -1L;

    try (CloseableIterable<Record> rows =
        Parquet.read(inputFile)
            .project(SCHEMA_WITH_POS)
            .filter(filter)
            .createReaderFunc(GenericParquetReaders::buildReader)
            .enablePageIndexFilteringForPoc()
            .build()) {

      for (Record row : rows) {
        long id = (Long) row.getField("id");
        long position = (Long) row.getField(MetadataColumns.ROW_POSITION.name());

        /*
         * writeSortedFile writes:
         *
         *   id == physical file row position
         *
         * so this is a very strong correctness assertion.
         */
        assertThat(position).as("Physical position for id=%s", id).isEqualTo(id);

        if (candidateCount == 0L) {
          firstPosition = position;
        }

        if (previousPosition >= 0L && position > previousPosition + 1L) {
          sawGap = true;
        }

        if (id == 10_000L) {
          sawFirstTarget = true;
        }

        if (id == 50_000L) {
          sawSecondTarget = true;
        }

        previousPosition = position;
        lastPosition = position;
        candidateCount += 1L;
      }
    }

    assertThat(candidateCount).isGreaterThanOrEqualTo(200L).isLessThan(RECORD_COUNT);

    assertThat(sawGap)
        .as("Page-index positions must preserve gaps between selected ranges")
        .isTrue();

    assertThat(sawFirstTarget).isTrue();
    assertThat(sawSecondTarget).isTrue();

    LOG.info(
        "_pos POC: candidateRows={}, " + "firstPosition={}, lastPosition={}, sawGap={}",
        candidateCount,
        firstPosition,
        lastPosition,
        sawGap);
  }

  @Test
  public void testFallbackRowIdUsesPhysicalPositionWithPageIndex() throws IOException {
    File parquetFile = new File(tempDir, "page-index-fallback-row-id.parquet");
    writeSortedFile(parquetFile);
    InputFile inputFile = Files.localInput(parquetFile);

    Expression firstRange =
        Expressions.and(
            Expressions.greaterThanOrEqual("id", 10_000L), Expressions.lessThan("id", 10_100L));
    Expression secondRange =
        Expressions.and(
            Expressions.greaterThanOrEqual("id", 50_000L), Expressions.lessThan("id", 50_100L));
    Expression filter = Expressions.or(firstRange, secondRange);

    Map<Integer, Object> constants = ImmutableMap.of(MetadataColumns.ROW_ID.fieldId(), BASE_ROW_ID);

    long candidateCount = 0L;
    boolean sawGap = false;
    long previousPosition = -1L;

    try (CloseableIterable<Record> rows =
        Parquet.read(inputFile)
            .project(SCHEMA_WITH_POS_AND_ROW_ID)
            .filter(filter)
            .createReaderFunc(
                (expectedSchema, fileSchema) ->
                    GenericParquetReaders.buildReader(expectedSchema, fileSchema, constants))
            .enablePageIndexFilteringForPoc()
            .build()) {

      for (Record row : rows) {
        long id = (Long) row.getField("id");
        long position = (Long) row.getField(MetadataColumns.ROW_POSITION.name());
        long rowId = (Long) row.getField(MetadataColumns.ROW_ID.name());

        assertThat(position).as("_pos for id=%s", id).isEqualTo(id);
        assertThat(rowId)
            .as("Fallback _row_id for physical position %s", position)
            .isEqualTo(BASE_ROW_ID + position);

        if (previousPosition >= 0L && position > previousPosition + 1L) {
          sawGap = true;
        }

        previousPosition = position;
        candidateCount += 1L;
      }
    }

    assertThat(candidateCount).isGreaterThanOrEqualTo(200L).isLessThan(RECORD_COUNT);
    assertThat(sawGap).isTrue();

    LOG.info(
        "Fallback _row_id POC: candidateRows={}, " + "baseRowId={}, sawGap={}",
        candidateCount,
        BASE_ROW_ID,
        sawGap);
  }

  @Test
  public void testPhysicalPositionsAcrossFilteredRowGroups() throws IOException {
    File parquetFile = new File(tempDir, "page-index-position-multiple-row-groups.parquet");
    writeMultiRowGroupFile(parquetFile);
    InputFile inputFile = Files.localInput(parquetFile);

    long secondRowGroupStart;
    long secondRowGroupCount;

    try (ParquetFileReader reader =
        ParquetFileReader.open(ParquetIO.file(inputFile), ParquetReadOptions.builder().build())) {

      assertThat(reader.getRowGroups().size()).isGreaterThan(1);
      secondRowGroupStart = reader.getRowGroups().get(0).getRowCount();
      secondRowGroupCount = reader.getRowGroups().get(1).getRowCount();
    }

    long targetStart = secondRowGroupStart + Math.min(1_000L, secondRowGroupCount / 2L);
    long targetEnd = Math.min(targetStart + 100L, secondRowGroupStart + secondRowGroupCount);

    Expression filter =
        Expressions.and(
            Expressions.greaterThanOrEqual("id", targetStart),
            Expressions.lessThan("id", targetEnd));

    long candidateCount = 0L;
    boolean sawTargetStart = false;
    boolean sawTargetEnd = false;

    long firstPosition = -1L;
    long lastPosition = -1L;

    try (CloseableIterable<Record> rows =
        Parquet.read(inputFile)
            .project(SCHEMA_WITH_POS)
            .filter(filter)
            .createReaderFunc(GenericParquetReaders::buildReader)
            .enablePageIndexFilteringForPoc()
            .build()) {

      for (Record row : rows) {
        long id = (Long) row.getField("id");
        long position = (Long) row.getField(MetadataColumns.ROW_POSITION.name());

        /*
         * id is the physical file position in this test file.
         */
        assertThat(position).isEqualTo(id);
        assertThat(position).isGreaterThanOrEqualTo(secondRowGroupStart);
        assertThat(position).isLessThan(secondRowGroupStart + secondRowGroupCount);

        if (candidateCount == 0L) {
          firstPosition = position;
        }

        lastPosition = position;

        if (position == targetStart) {
          sawTargetStart = true;
        }

        if (position == targetEnd - 1L) {
          sawTargetEnd = true;
        }

        candidateCount += 1L;
      }
    }

    assertThat(candidateCount)
        .isGreaterThanOrEqualTo(targetEnd - targetStart)
        .isLessThan(secondRowGroupCount);

    assertThat(sawTargetStart).isTrue();
    assertThat(sawTargetEnd).isTrue();

    LOG.info(
        "Multiple row groups: secondRowGroupStart={}, "
            + "target=[{},{}), candidateRows={}, "
            + "physicalPositions=[{},{}]",
        secondRowGroupStart,
        targetStart,
        targetEnd,
        candidateCount,
        firstPosition,
        lastPosition);
  }

  @Test
  public void testIcebergReaderHandlesZeroCandidateRowGroup() throws IOException {

    File parquetFile = new File(tempDir, "page-index-zero-candidate.parquet");
    writeFileWithPageGap(parquetFile);
    InputFile inputFile = Files.localInput(parquetFile);
    Expression filter = Expressions.equal("id", 1_500L);

    long candidateCount = 0L;

    try (CloseableIterable<Record> rows =
        Parquet.read(inputFile)
            .project(SCHEMA)
            .filter(filter)
            .createReaderFunc(GenericParquetReaders::buildReader)
            .enablePageIndexFilteringForPoc()
            .build()) {

      for (Record ignored : rows) {
        candidateCount += 1L;
      }
    }

    assertThat(candidateCount).isZero();

    LOG.info("Zero-candidate row group handled correctly: candidateRows={}", candidateCount);
  }

  @Test
  public void testPositionReaderStillWorksWithoutPageIndex() throws IOException {
    File parquetFile = new File(tempDir, "position-without-page-index.parquet");
    writeSortedFile(parquetFile);
    InputFile inputFile = Files.localInput(parquetFile);

    long count = 0L;

    try (CloseableIterable<Record> rows =
        Parquet.read(inputFile)
            .project(SCHEMA_WITH_POS)
            .createReaderFunc(GenericParquetReaders::buildReader)
            .build()) {

      for (Record row : rows) {
        long id = (Long) row.getField("id");
        long position = (Long) row.getField(MetadataColumns.ROW_POSITION.name());
        assertThat(position).isEqualTo(id);
        count += 1L;
      }
    }

    assertThat(count).isEqualTo(RECORD_COUNT);
  }

  private static void writeSortedFile(File parquetFile) throws IOException {
    OutputFile outputFile = Files.localOutput(parquetFile);

    try (FileAppender<Record> appender =
        Parquet.write(outputFile)
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(256 * 1024 * 1024))
            .set(TableProperties.PARQUET_PAGE_SIZE_BYTES, Integer.toString(64 * 1024))
            .set(TableProperties.PARQUET_PAGE_ROW_LIMIT, "1000")
            .set(TableProperties.PARQUET_COLUMN_STATS_ENABLED_PREFIX + "id", "true")
            .build()) {

      for (long id = 0L; id < RECORD_COUNT; id += 1L) {
        GenericRecord record = GenericRecord.create(SCHEMA);
        record.setField("id", id);
        record.setField("group_id", (int) (id / 10_000L));
        record.setField("payload", "payload-" + id);

        appender.add(record);
      }
    }
  }

  private static void writeMultiRowGroupFile(File parquetFile) throws IOException {
    OutputFile outputFile = Files.localOutput(parquetFile);

    try (FileAppender<Record> appender =
        Parquet.write(outputFile)
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(512 * 1024))
            .set(TableProperties.PARQUET_PAGE_SIZE_BYTES, Integer.toString(32 * 1024))
            .set(TableProperties.PARQUET_PAGE_ROW_LIMIT, "500")
            .set(TableProperties.PARQUET_COMPRESSION, "uncompressed")
            .set(TableProperties.PARQUET_COLUMN_STATS_ENABLED_PREFIX + "id", "true")
            .withDictionaryEncoding("id", false)
            .build()) {

      String payload = "x".repeat(256);

      for (long id = 0L; id < RECORD_COUNT; id += 1L) {
        GenericRecord record = GenericRecord.create(SCHEMA);
        record.setField("id", id);
        record.setField("group_id", (int) (id / 10_000L));
        record.setField("payload", payload + id);

        appender.add(record);
      }
    }
  }

  private static void writeFileWithPageGap(File parquetFile) throws IOException {

    OutputFile outputFile = Files.localOutput(parquetFile);

    try (FileAppender<Record> appender =
        Parquet.write(outputFile)
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .set(TableProperties.PARQUET_PAGE_ROW_LIMIT, "1000")
            .set(TableProperties.PARQUET_PAGE_SIZE_BYTES, Integer.toString(1024 * 1024))
            .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(128 * 1024 * 1024))
            .set(TableProperties.PARQUET_COLUMN_STATS_ENABLED_PREFIX + "id", "true")
            .withDictionaryEncoding("id", false)
            .build()) {

      for (long id = 0L; id < 1_000L; id += 1L) {
        GenericRecord record = GenericRecord.create(SCHEMA);

        record.setField("id", id);
        record.setField("group_id", 0);
        record.setField("payload", "low-" + id);

        appender.add(record);
      }

      for (long id = 2_000L; id < 3_000L; id += 1L) {
        GenericRecord record = GenericRecord.create(SCHEMA);

        record.setField("id", id);
        record.setField("group_id", 1);
        record.setField("payload", "high-" + id);

        appender.add(record);
      }
    }
  }

  private static class CountingInputFile implements InputFile {
    private final InputFile delegate;
    private final AtomicLong bytesRead = new AtomicLong();

    private CountingInputFile(InputFile delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getLength() {
      return delegate.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      return new CountingSeekableInputStream(delegate.newStream(), bytesRead);
    }

    @Override
    public String location() {
      return delegate.location();
    }

    @Override
    public boolean exists() {
      return delegate.exists();
    }

    long bytesRead() {
      return bytesRead.get();
    }

    void reset() {
      bytesRead.set(0L);
    }
  }

  private static class CountingSeekableInputStream extends SeekableInputStream {

    private final SeekableInputStream delegate;
    private final AtomicLong bytesRead;

    private CountingSeekableInputStream(SeekableInputStream delegate, AtomicLong bytesRead) {
      this.delegate = delegate;
      this.bytesRead = bytesRead;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      delegate.seek(newPos);
    }

    @Override
    public int read() throws IOException {
      int value = delegate.read();
      if (value >= 0) {
        bytesRead.incrementAndGet();
      }
      return value;
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
      int read = delegate.read(buffer, offset, length);
      if (read > 0) {
        bytesRead.addAndGet(read);
      }
      return read;
    }

    @Override
    public void close() throws IOException {
      delegate.close();
    }
  }
}
