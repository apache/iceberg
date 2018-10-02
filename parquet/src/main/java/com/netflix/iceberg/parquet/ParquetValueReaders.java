/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.parquet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.Type;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyIterator;

public class ParquetValueReaders {
  private ParquetValueReaders() {
  }

  public static <T> ParquetValueReader<T> option(Type type, int definitionLevel,
                                                 ParquetValueReader<T> reader) {
    if (type.isRepetition(Type.Repetition.OPTIONAL)) {
      return new OptionReader<>(definitionLevel, reader);
    }
    return reader;
  }

  @SuppressWarnings("unchecked")
  public static <T> ParquetValueReader<T> nulls() {
    return (ParquetValueReader<T>) NullReader.INSTANCE;
  }

  private static class NullReader<T> implements ParquetValueReader<T> {
    private static final NullReader<Void> INSTANCE = new NullReader<>();
    private static final List<TripleIterator<?>> COLUMNS = ImmutableList.of();
    private static final TripleIterator<?> NULL_COLUMN = new TripleIterator<Object> () {
      @Override
      public int currentDefinitionLevel() {
        return 0;
      }

      @Override
      public int currentRepetitionLevel() {
        return 0;
      }

      @Override
      public <N> N nextNull() {
        return null;
      }

      @Override
      public boolean hasNext() {
        return false;
      }

      @Override
      public Object next() {
        return null;
      }
    };

    private NullReader() {
    }

    @Override
    public T read(T reuse) {
      return null;
    }

    @Override
    public TripleIterator<?> column() {
      return NULL_COLUMN;
    }

    @Override
    public List<TripleIterator<?>> columns() {
      return COLUMNS;
    }

    @Override
    public void setPageSource(PageReadStore pageStore) {
    }
  }

  public abstract static class PrimitiveReader<T> implements ParquetValueReader<T> {
    private final ColumnDescriptor desc;
    protected final ColumnIterator<?> column;
    private final List<TripleIterator<?>> children;

    protected PrimitiveReader(ColumnDescriptor desc) {
      this.desc = desc;
      this.column = ColumnIterator.newIterator(desc, "");
      this.children = ImmutableList.of(column);
    }

    @Override
    public void setPageSource(PageReadStore pageStore) {
      column.setPageSource(pageStore.getPageReader(desc));
    }

    @Override
    public TripleIterator<?> column() {
      return column;
    }

    @Override
    public List<TripleIterator<?>> columns() {
      return children;
    }
  }

  public static class UnboxedReader<T> extends PrimitiveReader<T> {
    public UnboxedReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T read(T ignored) {
      return (T) column.next();
    }

    public boolean readBoolean() {
      return column.nextBoolean();
    }

    public int readInteger() {
      return column.nextInteger();
    }

    public long readLong() {
      return column.nextLong();
    }

    public float readFloat() {
      return column.nextFloat();
    }

    public double readDouble() {
      return column.nextDouble();
    }

    public Binary readBinary() {
      return column.nextBinary();
    }
  }

  public static class StringReader extends PrimitiveReader<String> {
    public StringReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public String read(String reuse) {
      return column.nextBinary().toStringUsingUTF8();
    }
  }

  public static class IntAsLongReader extends UnboxedReader<Long> {
    public IntAsLongReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public Long read(Long ignored) {
      return readLong();
    }

    @Override
    public long readLong() {
      return super.readInteger();
    }
  }

  public static class FloatAsDoubleReader extends UnboxedReader<Double> {
    public FloatAsDoubleReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public Double read(Double ignored) {
      return readDouble();
    }

    @Override
    public double readDouble() {
      return super.readFloat();
    }
  }

  public static class IntegerAsDecimalReader extends PrimitiveReader<BigDecimal> {
    private final int scale;

    public IntegerAsDecimalReader(ColumnDescriptor desc, int scale) {
      super(desc);
      this.scale = scale;
    }

    @Override
    public BigDecimal read(BigDecimal ignored) {
      return new BigDecimal(BigInteger.valueOf(column.nextInteger()), scale);
    }
  }

  public static class LongAsDecimalReader extends PrimitiveReader<BigDecimal> {
    private final int scale;

    public LongAsDecimalReader(ColumnDescriptor desc, int scale) {
      super(desc);
      this.scale = scale;
    }

    @Override
    public BigDecimal read(BigDecimal ignored) {
      return new BigDecimal(BigInteger.valueOf(column.nextLong()), scale);
    }
  }

  public static class BinaryAsDecimalReader extends PrimitiveReader<BigDecimal> {
    private int scale;

    public BinaryAsDecimalReader(ColumnDescriptor desc, int scale) {
      super(desc);
      this.scale = scale;
    }

    @Override
    public BigDecimal read(BigDecimal reuse) {
      byte[] bytes = column.nextBinary().getBytesUnsafe();
      return new BigDecimal(new BigInteger(bytes), scale);
    }
  }

  public static class BytesReader extends PrimitiveReader<ByteBuffer> {
    public BytesReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public ByteBuffer read(ByteBuffer reuse) {
      Binary binary = column.nextBinary();
      ByteBuffer data = binary.toByteBuffer();
      if (reuse != null && reuse.hasArray() && reuse.capacity() >= data.remaining()) {
        data.get(reuse.array(), reuse.arrayOffset(), data.remaining());
        reuse.position(0);
        reuse.limit(data.remaining());
        return reuse;
      } else {
        byte[] array = new byte[data.remaining()];
        data.get(array, 0, data.remaining());
        return ByteBuffer.wrap(array);
      }
    }
  }

  private static class OptionReader<T> implements ParquetValueReader<T> {
    private final int definitionLevel;
    private final ParquetValueReader<T> reader;
    private final TripleIterator<?> column;
    private final List<TripleIterator<?>> children;

    OptionReader(int definitionLevel, ParquetValueReader<T> reader) {
      this.definitionLevel = definitionLevel;
      this.reader = reader;
      this.column = reader.column();
      this.children = reader.columns();
    }

    @Override
    public void setPageSource(PageReadStore pageStore) {
      reader.setPageSource(pageStore);
    }

    @Override
    public TripleIterator<?> column() {
      return column;
    }

    @Override
    public T read(T reuse) {
      if (column.currentDefinitionLevel() > definitionLevel) {
        return reader.read(reuse);
      }

      for (TripleIterator<?> column : children) {
        column.nextNull();
      }

      return null;
    }

    @Override
    public List<TripleIterator<?>> columns() {
      return children;
    }
  }

  public abstract static class RepeatedReader<T, I, E> implements ParquetValueReader<T> {
    private final int definitionLevel;
    private final int repetitionLevel;
    private final ParquetValueReader<E> reader;
    private final TripleIterator<?> column;
    private final List<TripleIterator<?>> children;

    protected RepeatedReader(int definitionLevel, int repetitionLevel, ParquetValueReader<E> reader) {
      this.definitionLevel = definitionLevel;
      this.repetitionLevel = repetitionLevel;
      this.reader = reader;
      this.column = reader.column();
      this.children = reader.columns();
    }

    @Override
    public void setPageSource(PageReadStore pageStore) {
      reader.setPageSource(pageStore);
    }

    @Override
    public TripleIterator<?> column() {
      return column;
    }

    @Override
    public T read(T reuse) {
      I intermediate = newListData(reuse);

      do {
        if (column.currentDefinitionLevel() > definitionLevel) {
          addElement(intermediate, reader.read(getElement(intermediate)));
        } else {
          // consume the empty list triple
          for (TripleIterator<?> column : children) {
            column.nextNull();
          }
          // if the current definition level is equal to the definition level of this repeated type,
          // then the result is an empty list and the repetition level will always be <= rl.
          break;
        }
      } while (column.currentRepetitionLevel() > repetitionLevel);

      return buildList(intermediate);
    }

    @Override
    public List<TripleIterator<?>> columns() {
      return children;
    }

    protected abstract I newListData(T reuse);

    protected abstract E getElement(I list);

    protected abstract void addElement(I list, E element);

    protected abstract T buildList(I list);
  }

  public static class ListReader<E> extends RepeatedReader<List<E>, List<E>, E> {
    private List<E> lastList = null;
    private Iterator<E> elements = null;

    public ListReader(int definitionLevel, int repetitionLevel,
                      ParquetValueReader<E> reader) {
      super(definitionLevel, repetitionLevel, reader);
    }

    @Override
    protected List<E> newListData(List<E> reuse) {
      List<E> list;
      if (lastList != null) {
        lastList.clear();
        list = lastList;
      } else {
        list = Lists.newArrayList();
      }

      if (reuse != null) {
        this.lastList = reuse;
        this.elements = reuse.iterator();
      } else {
        this.lastList = null;
        this.elements = emptyIterator();
      }

      return list;
    }

    @Override
    protected E getElement(List<E> reuse) {
      if (elements.hasNext()) {
        return elements.next();
      }

      return null;
    }

    @Override
    protected void addElement(List<E> list, E element) {
      list.add(element);
    }

    @Override
    protected List<E> buildList(List<E> list) {
      return list;
    }
  }

  public abstract static class RepeatedKeyValueReader<M, I, K, V> implements ParquetValueReader<M> {
    private final int definitionLevel;
    private final int repetitionLevel;
    private final ParquetValueReader<K> keyReader;
    private final ParquetValueReader<V> valueReader;
    private final TripleIterator<?> column;
    private final List<TripleIterator<?>> children;

    protected RepeatedKeyValueReader(int definitionLevel, int repetitionLevel,
                           ParquetValueReader<K> keyReader, ParquetValueReader<V> valueReader) {
      this.definitionLevel = definitionLevel;
      this.repetitionLevel = repetitionLevel;
      this.keyReader = keyReader;
      this.valueReader = valueReader;
      this.column = keyReader.column();
      this.children = ImmutableList.<TripleIterator<?>>builder()
          .addAll(keyReader.columns())
          .addAll(valueReader.columns())
          .build();
    }

    @Override
    public void setPageSource(PageReadStore pageStore) {
      keyReader.setPageSource(pageStore);
      valueReader.setPageSource(pageStore);
    }

    @Override
    public TripleIterator<?> column() {
      return column;
    }

    @Override
    public M read(M reuse) {
      I intermediate = newMapData(reuse);

      do {
        if (column.currentDefinitionLevel() > definitionLevel) {
          Map.Entry<K, V> pair = getPair(intermediate);
          addPair(intermediate, keyReader.read(pair.getKey()), valueReader.read(pair.getValue()));
        } else {
          // consume the empty map triple
          for (TripleIterator<?> column : children) {
            column.nextNull();
          }
          // if the current definition level is equal to the definition level of this repeated type,
          // then the result is an empty list and the repetition level will always be <= rl.
          break;
        }
      } while (column.currentRepetitionLevel() > repetitionLevel);

      return buildMap(intermediate);
    }

    @Override
    public List<TripleIterator<?>> columns() {
      return children;
    }

    protected abstract I newMapData(M reuse);

    protected abstract Map.Entry<K, V> getPair(I map);

    protected abstract void addPair(I map, K key, V value);

    protected abstract M buildMap(I map);
  }

  public static class MapReader<K, V> extends RepeatedKeyValueReader<Map<K, V>, Map<K, V>, K, V> {
    private final ReusableEntry<K, V> nullEntry = new ReusableEntry<>();
    private Map<K, V> lastMap = null;
    private Iterator<Map.Entry<K, V>> pairs = null;

    public MapReader(int definitionLevel, int repetitionLevel,
                     ParquetValueReader<K> keyReader,
                     ParquetValueReader<V> valueReader) {
      super(definitionLevel, repetitionLevel, keyReader, valueReader);
    }

    @Override
    protected Map<K, V> newMapData(Map<K, V> reuse) {
      Map<K, V> map;
      if (lastMap != null) {
        lastMap.clear();
        map = lastMap;
      } else {
        map = Maps.newLinkedHashMap();
      }

      if (reuse != null) {
        this.lastMap = reuse;
        this.pairs = reuse.entrySet().iterator();
      } else {
        this.lastMap = null;
        this.pairs = emptyIterator();
      }

      return map;
    }

    @Override
    protected Map.Entry<K, V> getPair(Map<K, V> map) {
      if (pairs.hasNext()) {
        return pairs.next();
      } else {
        return nullEntry;
      }
    }

    @Override
    protected void addPair(Map<K, V> map, K key, V value) {
      map.put(key, value);
    }

    @Override
    protected Map<K, V> buildMap(Map<K, V> map) {
      return map;
    }
  }

  public static class ReusableEntry<K, V> implements Map.Entry<K, V> {
    private K key = null;
    private V value = null;

    public void set(K key, V value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public K getKey() {
      return key;
    }

    @Override
    public V getValue() {
      return value;
    }

    @Override
    public V setValue(V value) {
      V lastValue = this.value;
      this.value = value;
      return lastValue;
    }
  }

  public abstract static class StructReader<T, I> implements ParquetValueReader<T> {
    private interface Setter<R> {
      void set(R record, int pos, Object reuse);
    }

    private final ParquetValueReader<?>[] readers;
    private final TripleIterator<?> column;
    private final TripleIterator<?>[] columns;
    private final Setter<I>[] setters;
    private final List<TripleIterator<?>> children;

    @SuppressWarnings("unchecked")
    protected StructReader(List<Type> types, List<ParquetValueReader<?>> readers) {
      this.readers = (ParquetValueReader<?>[]) Array.newInstance(
          ParquetValueReader.class, readers.size());
      this.columns = (TripleIterator<?>[]) Array.newInstance(TripleIterator.class, readers.size());
      this.setters = (Setter<I>[]) Array.newInstance(Setter.class, readers.size());

      ImmutableList.Builder<TripleIterator<?>> columnsBuilder = ImmutableList.builder();
      for (int i = 0; i < readers.size(); i += 1) {
        ParquetValueReader<?> reader = readers.get(i);
        this.readers[i] = readers.get(i);
        this.columns[i] = reader.column();
        this.setters[i] = newSetter(reader, types.get(i));
        columnsBuilder.addAll(reader.columns());
      }

      this.children = columnsBuilder.build();
      if (children.size() > 0) {
        this.column = children.get(0);
      } else {
        this.column = NullReader.NULL_COLUMN;
      }
    }

    @Override
    public final void setPageSource(PageReadStore pageStore) {
      for (int i = 0; i < readers.length; i += 1) {
        readers[i].setPageSource(pageStore);
      }
    }

    @Override
    public final TripleIterator<?> column() {
      return column;
    }

    @Override
    public final T read(T reuse) {
      I intermediate = newStructData(reuse);

      for (int i = 0; i < readers.length; i += 1) {
        set(intermediate, i, readers[i].read(get(intermediate, i)));
        //setters[i].set(intermediate, i, get(intermediate, i));
      }

      return buildStruct(intermediate);
    }

    @Override
    public List<TripleIterator<?>> columns() {
      return children;
    }

    @SuppressWarnings("unchecked")
    private <E> Setter<I> newSetter(ParquetValueReader<E> reader, Type type) {
      if (reader instanceof UnboxedReader && type.isPrimitive()) {
        UnboxedReader<?> unboxed  = (UnboxedReader<?>) reader;
        switch (type.asPrimitiveType().getPrimitiveTypeName()) {
          case BOOLEAN:
            return (record, pos, ignored) -> setBoolean(record, pos, unboxed.readBoolean());
          case INT32:
            return (record, pos, ignored) -> setInteger(record, pos, unboxed.readInteger());
          case INT64:
            return (record, pos, ignored) -> setLong(record, pos, unboxed.readLong());
          case FLOAT:
            return (record, pos, ignored) -> setFloat(record, pos, unboxed.readFloat());
          case DOUBLE:
            return (record, pos, ignored) -> setDouble(record, pos, unboxed.readDouble());
          case FIXED_LEN_BYTE_ARRAY:
          case BINARY:
            return (record, pos, ignored) -> set(record, pos, unboxed.readBinary());
          default:
            throw new UnsupportedOperationException("Unsupported type: " + type);
        }
      }

      // TODO: Add support for options to avoid the null check
      return (record, pos, reuse) -> {
        Object obj = reader.read((E) reuse);
        if (obj != null) {
          set(record, pos, obj);
        } else {
          setNull(record, pos);
        }
      };
    }

    @SuppressWarnings("unchecked")
    private <E> E get(I intermediate, int pos) {
      return (E) getField(intermediate, pos);
    }

    protected abstract I newStructData(T reuse);

    protected abstract Object getField(I intermediate, int pos);

    protected abstract T buildStruct(I struct);

    /**
     * Used to set a struct value by position.
     * <p>
     * To avoid boxing, override {@link #setInteger(Object, int, int)} and similar methods.
     *
     * @param struct a struct object created by {@link #newStructData(Object)}
     * @param pos the position in the struct to set
     * @param value the value to set
     */
    protected abstract void set(I struct, int pos, Object value);

    protected void setNull(I struct, int pos) {
      set(struct, pos, null);
    }

    protected void setBoolean(I struct, int pos, boolean value) {
      set(struct, pos, value);
    }

    protected void setInteger(I struct, int pos, int value) {
      set(struct, pos, value);
    }

    protected void setLong(I struct, int pos, long value) {
      set(struct, pos, value);
    }

    protected void setFloat(I struct, int pos, float value) {
      set(struct, pos, value);
    }

    protected void setDouble(I struct, int pos, double value) {
      set(struct, pos, value);
    }
  }
}
