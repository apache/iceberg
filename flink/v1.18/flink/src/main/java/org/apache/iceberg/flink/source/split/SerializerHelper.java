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

package org.apache.iceberg.flink.source.split;

import java.io.IOException;
import java.io.Serializable;
import java.io.UTFDataFormatException;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

/**
 * Helper class to serialize and deserialize strings longer than 65K. The inspiration is mostly
 * taken from the class org.apache.flink.core.memory.DataInputSerializer.readUTF and
 * org.apache.flink.core.memory.DataOutputSerializer.writeUTF.
 */
public class SerializerHelper implements Serializable {

  /**
   * Similar to {@link DataOutputSerializer#writeUTF(String)}. The size is only limited by the
   * maximum java array size of the buffer.
   *
   * @param out the output stream to write the string to.
   * @param str the string value to be written.
   * @deprecated This method is deprecated because there will be a method within the {@link
   *     DataOutputSerializer} already which does the same thing, so use that one instead once that
   *     is released on Flink version 1.20.
   *     <p>See * <a href="https://issues.apache.org/jira/browse/FLINK-34228">FLINK-34228</a> * <a
   *     href="https://github.com/apache/flink/pull/24191">https://github.com/apache/flink/pull/24191</a>
   */
  @Deprecated
  public static void writeLongUTF(DataOutputSerializer out, String str) throws IOException {
    int strlen = str.length();
    long utflen = 0;
    int c;

    /* use charAt instead of copying String to char array */
    for (int i = 0; i < strlen; i++) {
      c = str.charAt(i);
      utflen += getUTFBytesSize(c);

      if (utflen > Integer.MAX_VALUE) {
        throw new UTFDataFormatException("Encoded string reached maximum length: " + utflen);
      }
    }
    if (utflen > Integer.MAX_VALUE - 4) {
      throw new UTFDataFormatException("Encoded string is too long: " + utflen);
    }

    out.writeInt((int) utflen);
    writeUTFBytes(out, str, (int) utflen);
  }

  public static String readLongUTF(DataInputDeserializer in) throws IOException {
    int utflen = in.readInt();
    byte[] bytearr = new byte[utflen];
    char[] chararr = new char[utflen];

    int c, char2, char3;
    int count = 0;
    int chararrCount = 0;

    in.readFully(bytearr, 0, utflen);

    while (count < utflen) {
      c = (int) bytearr[count] & 0xff;
      if (c > 127) {
        break;
      }
      count++;
      chararr[chararrCount++] = (char) c;
    }

    while (count < utflen) {
      c = (int) bytearr[count] & 0xff;
      switch (c >> 4) {
        case 0:
        case 1:
        case 2:
        case 3:
        case 4:
        case 5:
        case 6:
        case 7:
          /* 0xxxxxxx */
          count++;
          chararr[chararrCount++] = (char) c;
          break;
        case 12:
        case 13:
          /* 110x xxxx 10xx xxxx */
          count += 2;
          if (count > utflen) {
            throw new UTFDataFormatException("malformed input: partial character at end");
          }
          char2 = (int) bytearr[count - 1];
          if ((char2 & 0xC0) != 0x80) {
            throw new UTFDataFormatException("malformed input around byte " + count);
          }
          chararr[chararrCount++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
          break;
        case 14:
          /* 1110 xxxx 10xx xxxx 10xx xxxx */
          count += 3;
          if (count > utflen) {
            throw new UTFDataFormatException("malformed input: partial character at end");
          }
          char2 = (int) bytearr[count - 2];
          char3 = (int) bytearr[count - 1];
          if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
            throw new UTFDataFormatException("malformed input around byte " + (count - 1));
          }
          chararr[chararrCount++] =
              (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | (char3 & 0x3F));
          break;
        default:
          /* 10xx xxxx, 1111 xxxx */
          throw new UTFDataFormatException("malformed input around byte " + count);
      }
    }
    // The number of chars produced may be less than utflen
    return new String(chararr, 0, chararrCount);
  }

  private static int getUTFBytesSize(int c) {
    if ((c >= 0x0001) && (c <= 0x007F)) {
      return 1;
    } else if (c > 0x07FF) {
      return 3;
    } else {
      return 2;
    }
  }

  private static void writeUTFBytes(DataOutputSerializer out, String str, int utflen)
      throws IOException {
    int strlen = str.length();
    int c;

    int len = Math.max(1024, utflen);

    byte[] bytearr = new byte[len];
    int count = 0;

    int i;
    for (i = 0; i < strlen; i++) {
      c = str.charAt(i);
      if (!((c >= 0x0001) && (c <= 0x007F))) {
        break;
      }
      bytearr[count++] = (byte) c;
    }

    for (; i < strlen; i++) {
      c = str.charAt(i);
      if ((c >= 0x0001) && (c <= 0x007F)) {
        bytearr[count++] = (byte) c;
      } else if (c > 0x07FF) {
        bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
        bytearr[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
        bytearr[count++] = (byte) (0x80 | (c & 0x3F));
      } else {
        bytearr[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
        bytearr[count++] = (byte) (0x80 | (c & 0x3F));
      }
    }

    out.write(bytearr, 0, count);
  }
}
