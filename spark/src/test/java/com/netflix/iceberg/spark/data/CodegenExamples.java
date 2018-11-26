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

package com.netflix.iceberg.spark.data;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.catalyst.expressions.UnsafeMapData;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

public class CodegenExamples {


  class Example1 extends org.apache.spark.sql.catalyst.expressions.UnsafeProjection {

    private Object[] references;
    private UnsafeRow result;
    private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder holder;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter rowWriter;

    public Example1(Object[] references) {
      this.references = references;
      result = new UnsafeRow(2);
      this.holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(result, 32);
      this.rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(holder, 2);

    }

    public void initialize(int partitionIndex) {

    }

    public UnsafeRow apply(InternalRow i) {
      holder.reset();

      rowWriter.zeroOutNullBytes();


      boolean isNull = i.isNullAt(0);
      long value = isNull ? -1L : (i.getLong(0));
      if (isNull) {
        rowWriter.setNullAt(0);
      } else {
        rowWriter.write(0, value);
      }


      boolean isNull1 = i.isNullAt(1);
      UTF8String value1 = isNull1 ? null : (i.getUTF8String(1));
      if (isNull1) {
        rowWriter.setNullAt(1);
      } else {
        rowWriter.write(1, value1);
      }
      result.setTotalSize(holder.totalSize());
      return result;
    }
  }

  class Example2 extends org.apache.spark.sql.catalyst.expressions.UnsafeProjection {

    private Object[] references;
    private UnsafeRow result;
    private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder holder;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter rowWriter;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter rowWriter1;

    public Example2(Object[] references) {
      this.references = references;
      result = new UnsafeRow(1);
      this.holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(result, 32);
      this.rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(holder, 1);
      this.rowWriter1 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(holder, 1);

    }

    public void initialize(int partitionIndex) {

    }

    public UnsafeRow apply(InternalRow i) {
      holder.reset();

      rowWriter.zeroOutNullBytes();


      boolean isNull = i.isNullAt(0);
      InternalRow value = isNull ? null : (i.getStruct(0, 1));
      if (isNull) {
        rowWriter.setNullAt(0);
      } else {
        // Remember the current cursor so that we can calculate how many bytes are
        // written later.
        final int tmpCursor = holder.cursor;

        if (value instanceof UnsafeRow) {

          final int sizeInBytes = ((UnsafeRow) value).getSizeInBytes();
          // grow the global buffer before writing data.
          holder.grow(sizeInBytes);
          ((UnsafeRow) value).writeToMemory(holder.buffer, holder.cursor);
          holder.cursor += sizeInBytes;

        } else {
          rowWriter1.reset();


          boolean isNull1 = value.isNullAt(0);
          float value1 = isNull1 ? -1.0f : value.getFloat(0);

          if (isNull1) {
            rowWriter1.setNullAt(0);
          } else {
            rowWriter1.write(0, value1);
          }
        }

        rowWriter.setOffsetAndSize(0, tmpCursor, holder.cursor - tmpCursor);
      }
      result.setTotalSize(holder.totalSize());
      return result;
    }
  }

  class Example3 extends org.apache.spark.sql.catalyst.expressions.UnsafeProjection {

    private Object[] references;
    private UnsafeRow result;
    private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder holder;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter rowWriter;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter rowWriter1;

    public Example3(Object[] references) {
      this.references = references;
      result = new UnsafeRow(1);
      this.holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(result, 32);
      this.rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(holder, 1);
      this.rowWriter1 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(holder, 2);

    }

    public void initialize(int partitionIndex) {

    }

    public UnsafeRow apply(InternalRow i) {
      holder.reset();

      rowWriter.zeroOutNullBytes();


      boolean isNull = i.isNullAt(0);
      InternalRow value = isNull ? null : (i.getStruct(0, 2));
      if (isNull) {
        rowWriter.setNullAt(0);
      } else {
        // Remember the current cursor so that we can calculate how many bytes are
        // written later.
        final int tmpCursor = holder.cursor;

        if (value instanceof UnsafeRow) {

          final int sizeInBytes = ((UnsafeRow) value).getSizeInBytes();
          // grow the global buffer before writing data.
          holder.grow(sizeInBytes);
          ((UnsafeRow) value).writeToMemory(holder.buffer, holder.cursor);
          holder.cursor += sizeInBytes;

        } else {
          rowWriter1.reset();


          boolean isNull1 = value.isNullAt(0);
          float value1 = isNull1 ? -1.0f : value.getFloat(0);

          if (isNull1) {
            rowWriter1.setNullAt(0);
          } else {
            rowWriter1.write(0, value1);
          }


          boolean isNull2 = value.isNullAt(1);
          float value2 = isNull2 ? -1.0f : value.getFloat(1);

          if (isNull2) {
            rowWriter1.setNullAt(1);
          } else {
            rowWriter1.write(1, value2);
          }
        }

        rowWriter.setOffsetAndSize(0, tmpCursor, holder.cursor - tmpCursor);
      }
      result.setTotalSize(holder.totalSize());
      return result;
    }
  }


  class Example4 extends org.apache.spark.sql.catalyst.expressions.UnsafeProjection {

    private Object[] references;
    private UnsafeRow result;
    private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder holder;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter rowWriter;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter arrayWriter;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter arrayWriter1;

    public Example4(Object[] references) {
      this.references = references;
      result = new UnsafeRow(1);
      this.holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(result, 32);
      this.rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(holder, 1);
      this.arrayWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter();
      this.arrayWriter1 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter();

    }

    public void initialize(int partitionIndex) {

    }

    public UnsafeRow apply(InternalRow i) {
      holder.reset();

      rowWriter.zeroOutNullBytes();


      boolean isNull = i.isNullAt(0);
      MapData value = isNull ? null : (i.getMap(0));
      if (isNull) {
        rowWriter.setNullAt(0);
      } else {
        // Remember the current cursor so that we can calculate how many bytes are
        // written later.
        final int tmpCursor = holder.cursor;

        if (value instanceof UnsafeMapData) {

          final int sizeInBytes = ((UnsafeMapData) value).getSizeInBytes();
          // grow the global buffer before writing data.
          holder.grow(sizeInBytes);
          ((UnsafeMapData) value).writeToMemory(holder.buffer, holder.cursor);
          holder.cursor += sizeInBytes;

        } else {
          final ArrayData keys = value.keyArray();
          final ArrayData values = value.valueArray();

          // preserve 8 bytes to write the key array numBytes later.
          holder.grow(8);
          holder.cursor += 8;

          // Remember the current cursor so that we can write numBytes of key array later.
          final int tmpCursor1 = holder.cursor;


          if (keys instanceof UnsafeArrayData) {

            final int sizeInBytes1 = ((UnsafeArrayData) keys).getSizeInBytes();
            // grow the global buffer before writing data.
            holder.grow(sizeInBytes1);
            ((UnsafeArrayData) keys).writeToMemory(holder.buffer, holder.cursor);
            holder.cursor += sizeInBytes1;

          } else {
            final int numElements = keys.numElements();
            arrayWriter.initialize(holder, numElements, 8);

            for (int index = 0; index < numElements; index++) {
              if (keys.isNullAt(index)) {
                arrayWriter.setNull(index);
              } else {
                final UTF8String element = keys.getUTF8String(index);
                arrayWriter.write(index, element);
              }
            }
          }

          // Write the numBytes of key array into the first 8 bytes.
          Platform.putLong(holder.buffer, tmpCursor1 - 8, holder.cursor - tmpCursor1);


          if (values instanceof UnsafeArrayData) {

            final int sizeInBytes2 = ((UnsafeArrayData) values).getSizeInBytes();
            // grow the global buffer before writing data.
            holder.grow(sizeInBytes2);
            ((UnsafeArrayData) values).writeToMemory(holder.buffer, holder.cursor);
            holder.cursor += sizeInBytes2;

          } else {
            final int numElements1 = values.numElements();
            arrayWriter1.initialize(holder, numElements1, 8);

            for (int index1 = 0; index1 < numElements1; index1++) {
              if (values.isNullAt(index1)) {
                arrayWriter1.setNull(index1);
              } else {
                final UTF8String element1 = values.getUTF8String(index1);
                arrayWriter1.write(index1, element1);
              }
            }
          }

        }

        rowWriter.setOffsetAndSize(0, tmpCursor, holder.cursor - tmpCursor);
      }
      result.setTotalSize(holder.totalSize());
      return result;
    }
  }


  class Example5 extends org.apache.spark.sql.catalyst.expressions.UnsafeProjection {

    private Object[] references;
    private UnsafeRow result;
    private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder holder;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter rowWriter;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter arrayWriter;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter rowWriter1;

    public Example5(Object[] references) {
      this.references = references;
      result = new UnsafeRow(1);
      this.holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(result, 32);
      this.rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(holder, 1);
      this.arrayWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter();
      this.rowWriter1 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(holder, 2);

    }

    public void initialize(int partitionIndex) {

    }

    public UnsafeRow apply(InternalRow i) {
      holder.reset();

      rowWriter.zeroOutNullBytes();


      boolean isNull = i.isNullAt(0);
      ArrayData value = isNull ? null : (i.getArray(0));
      if (isNull) {
        rowWriter.setNullAt(0);
      } else {
        // Remember the current cursor so that we can calculate how many bytes are
        // written later.
        final int tmpCursor = holder.cursor;

        if (value instanceof UnsafeArrayData) {

          final int sizeInBytes1 = ((UnsafeArrayData) value).getSizeInBytes();
          // grow the global buffer before writing data.
          holder.grow(sizeInBytes1);
          ((UnsafeArrayData) value).writeToMemory(holder.buffer, holder.cursor);
          holder.cursor += sizeInBytes1;

        } else {
          final int numElements = value.numElements();
          arrayWriter.initialize(holder, numElements, 8);

          for (int index = 0; index < numElements; index++) {
            if (value.isNullAt(index)) {
              arrayWriter.setNull(index);
            } else {
              final InternalRow element = value.getStruct(index, 2);

              final int tmpCursor1 = holder.cursor;

              if (element instanceof UnsafeRow) {

                final int sizeInBytes = ((UnsafeRow) element).getSizeInBytes();
                // grow the global buffer before writing data.
                holder.grow(sizeInBytes);
                ((UnsafeRow) element).writeToMemory(holder.buffer, holder.cursor);
                holder.cursor += sizeInBytes;

              } else {
                rowWriter1.reset();


                boolean isNull1 = element.isNullAt(0);
                int value1 = isNull1 ? -1 : element.getInt(0);

                if (isNull1) {
                  rowWriter1.setNullAt(0);
                } else {
                  rowWriter1.write(0, value1);
                }


                boolean isNull2 = element.isNullAt(1);
                int value2 = isNull2 ? -1 : element.getInt(1);

                if (isNull2) {
                  rowWriter1.setNullAt(1);
                } else {
                  rowWriter1.write(1, value2);
                }
              }

              arrayWriter.setOffsetAndSize(index, tmpCursor1, holder.cursor - tmpCursor1);

            }
          }
        }

        rowWriter.setOffsetAndSize(0, tmpCursor, holder.cursor - tmpCursor);
      }
      result.setTotalSize(holder.totalSize());
      return result;
    }
  }

  class Example6 extends org.apache.spark.sql.catalyst.expressions.UnsafeProjection {

    private Object[] references;
    private UnsafeRow result;
    private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder holder;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter rowWriter;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter arrayWriter;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter arrayWriter1;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter rowWriter1;

    public Example6(Object[] references) {
      this.references = references;
      result = new UnsafeRow(1);
      this.holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(result, 32);
      this.rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(holder, 1);
      this.arrayWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter();
      this.arrayWriter1 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter();
      this.rowWriter1 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(holder, 2);

    }

    public void initialize(int partitionIndex) {

    }

    public UnsafeRow apply(InternalRow i) {
      holder.reset();

      rowWriter.zeroOutNullBytes();


      boolean isNull = i.isNullAt(0);
      MapData value = isNull ? null : (i.getMap(0));
      if (isNull) {
        rowWriter.setNullAt(0);
      } else {
        // Remember the current cursor so that we can calculate how many bytes are
        // written later.
        final int tmpCursor = holder.cursor;

        if (value instanceof UnsafeMapData) {

          final int sizeInBytes = ((UnsafeMapData) value).getSizeInBytes();
          // grow the global buffer before writing data.
          holder.grow(sizeInBytes);
          ((UnsafeMapData) value).writeToMemory(holder.buffer, holder.cursor);
          holder.cursor += sizeInBytes;

        } else {
          final ArrayData keys = value.keyArray();
          final ArrayData values = value.valueArray();

          // preserve 8 bytes to write the key array numBytes later.
          holder.grow(8);
          holder.cursor += 8;

          // Remember the current cursor so that we can write numBytes of key array later.
          final int tmpCursor1 = holder.cursor;


          if (keys instanceof UnsafeArrayData) {

            final int sizeInBytes1 = ((UnsafeArrayData) keys).getSizeInBytes();
            // grow the global buffer before writing data.
            holder.grow(sizeInBytes1);
            ((UnsafeArrayData) keys).writeToMemory(holder.buffer, holder.cursor);
            holder.cursor += sizeInBytes1;

          } else {
            final int numElements = keys.numElements();
            arrayWriter.initialize(holder, numElements, 8);

            for (int index = 0; index < numElements; index++) {
              if (keys.isNullAt(index)) {
                arrayWriter.setNull(index);
              } else {
                final UTF8String element = keys.getUTF8String(index);
                arrayWriter.write(index, element);
              }
            }
          }

          // Write the numBytes of key array into the first 8 bytes.
          Platform.putLong(holder.buffer, tmpCursor1 - 8, holder.cursor - tmpCursor1);


          if (values instanceof UnsafeArrayData) {

            final int sizeInBytes3 = ((UnsafeArrayData) values).getSizeInBytes();
            // grow the global buffer before writing data.
            holder.grow(sizeInBytes3);
            ((UnsafeArrayData) values).writeToMemory(holder.buffer, holder.cursor);
            holder.cursor += sizeInBytes3;

          } else {
            final int numElements1 = values.numElements();
            arrayWriter1.initialize(holder, numElements1, 8);

            for (int index1 = 0; index1 < numElements1; index1++) {
              if (values.isNullAt(index1)) {
                arrayWriter1.setNull(index1);
              } else {
                final InternalRow element1 = values.getStruct(index1, 2);

                final int tmpCursor3 = holder.cursor;

                if (element1 instanceof UnsafeRow) {

                  final int sizeInBytes2 = ((UnsafeRow) element1).getSizeInBytes();
                  // grow the global buffer before writing data.
                  holder.grow(sizeInBytes2);
                  ((UnsafeRow) element1).writeToMemory(holder.buffer, holder.cursor);
                  holder.cursor += sizeInBytes2;

                } else {
                  rowWriter1.reset();


                  boolean isNull1 = element1.isNullAt(0);
                  float value1 = isNull1 ? -1.0f : element1.getFloat(0);

                  if (isNull1) {
                    rowWriter1.setNullAt(0);
                  } else {
                    rowWriter1.write(0, value1);
                  }


                  boolean isNull2 = element1.isNullAt(1);
                  float value2 = isNull2 ? -1.0f : element1.getFloat(1);

                  if (isNull2) {
                    rowWriter1.setNullAt(1);
                  } else {
                    rowWriter1.write(1, value2);
                  }
                }

                arrayWriter1.setOffsetAndSize(index1, tmpCursor3, holder.cursor - tmpCursor3);

              }
            }
          }

        }

        rowWriter.setOffsetAndSize(0, tmpCursor, holder.cursor - tmpCursor);
      }
      result.setTotalSize(holder.totalSize());
      return result;
    }
  }

  class Example7 extends org.apache.spark.sql.catalyst.expressions.UnsafeProjection {

    private Object[] references;
    private UnsafeRow result;
    private org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder holder;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter rowWriter;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter arrayWriter;
    private org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter arrayWriter1;

    public Example7(Object[] references) {
      this.references = references;
      result = new UnsafeRow(1);
      this.holder = new org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder(result, 32);
      this.rowWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter(holder, 1);
      this.arrayWriter = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter();
      this.arrayWriter1 = new org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter();

    }

    public void initialize(int partitionIndex) {

    }

    public UnsafeRow apply(InternalRow i) {
      holder.reset();

      rowWriter.zeroOutNullBytes();


      boolean isNull = i.isNullAt(0);
      MapData value = isNull ? null : (i.getMap(0));
      if (isNull) {
        rowWriter.setNullAt(0);
      } else {
        // Remember the current cursor so that we can calculate how many bytes are
        // written later.
        final int tmpCursor = holder.cursor;

        if (value instanceof UnsafeMapData) {

          final int sizeInBytes = ((UnsafeMapData) value).getSizeInBytes();
          // grow the global buffer before writing data.
          holder.grow(sizeInBytes);
          ((UnsafeMapData) value).writeToMemory(holder.buffer, holder.cursor);
          holder.cursor += sizeInBytes;

        } else {
          final ArrayData keys = value.keyArray();
          final ArrayData values = value.valueArray();

          // preserve 8 bytes to write the key array numBytes later.
          holder.grow(8);
          holder.cursor += 8;

          // Remember the current cursor so that we can write numBytes of key array later.
          final int tmpCursor1 = holder.cursor;


          if (keys instanceof UnsafeArrayData) {

            final int sizeInBytes1 = ((UnsafeArrayData) keys).getSizeInBytes();
            // grow the global buffer before writing data.
            holder.grow(sizeInBytes1);
            ((UnsafeArrayData) keys).writeToMemory(holder.buffer, holder.cursor);
            holder.cursor += sizeInBytes1;

          } else {
            final int numElements = keys.numElements();
            arrayWriter.initialize(holder, numElements, 8);

            for (int index = 0; index < numElements; index++) {
              if (keys.isNullAt(index)) {
                arrayWriter.setNull(index);
              } else {
                final UTF8String element = keys.getUTF8String(index);
                arrayWriter.write(index, element);
              }
            }
          }

          // Write the numBytes of key array into the first 8 bytes.
          Platform.putLong(holder.buffer, tmpCursor1 - 8, holder.cursor - tmpCursor1);


          if (values instanceof UnsafeArrayData) {

            final int sizeInBytes2 = ((UnsafeArrayData) values).getSizeInBytes();
            // grow the global buffer before writing data.
            holder.grow(sizeInBytes2);
            ((UnsafeArrayData) values).writeToMemory(holder.buffer, holder.cursor);
            holder.cursor += sizeInBytes2;

          } else {
            final int numElements1 = values.numElements();
            arrayWriter1.initialize(holder, numElements1, 8);

            for (int index1 = 0; index1 < numElements1; index1++) {
              if (values.isNullAt(index1)) {
                arrayWriter1.setNull(index1);
              } else {
                final UTF8String element1 = values.getUTF8String(index1);
                arrayWriter1.write(index1, element1);
              }
            }
          }

        }

        rowWriter.setOffsetAndSize(0, tmpCursor, holder.cursor - tmpCursor);
      }
      result.setTotalSize(holder.totalSize());
      return result;
    }
  }
}
