package com.rapid7.armor.schema;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.roaringbitmap.RoaringBitmap;

public enum DataType {
  INTEGER(4, "I"), // rbitmap
  DOUBLE(8, "D"),
  STRING(4, "S"),    // Dictionary
  DATETIME(8, "DT"), // 64 bitmap
  FLOAT(4, "F"),
  LONG(8, "L"),      // 64 bitmap
  BOOLEAN(2, "B");

  private final int byteLength;
  private final String code;

  DataType(int bytes, String code) {
    this.byteLength = bytes;
    this.code = code;
  }

  public static DataType getDataType(String code) {
    for (DataType dt : DataType.values()) {
      if (dt.getCode().equals(code))
        return dt;
    }
    return null;
  }

  public String getCode() {
    return code;
  }

  public int rowCount(int usedBytes) {
    return usedBytes / byteLength;
  }

  public int getByteLength() {
    return this.byteLength;
  }

  public int determineNumValues(int valueByteLength) {
    return valueByteLength / byteLength;
  }

  public int determineByteLength(int numValues) {
    return byteLength * numValues;
  }

  public int writeValuesToByteBuffer(ByteBuffer byteBuffer, Set<Integer> nullPositions, Object[] values) {
    int counter = 1;
    for (Object value : values) {
      if (value == null) {
        switch (this) {
          case STRING:
            byteBuffer.putInt(0);
            continue;
          case LONG:
          case DATETIME:
            byteBuffer.putLong(0L);
            break;
          case FLOAT:
            byteBuffer.putFloat(0f);
            break;
          case INTEGER:
            byteBuffer.putInt(0);
            break;
          case BOOLEAN:
            byteBuffer.put((byte) 0);
            break;
          case DOUBLE:
            byteBuffer.putDouble(0d);
            break;
        }
        nullPositions.add(counter);
      } else {
        switch (this) {
          case LONG:
          case DATETIME:
            byteBuffer.putLong((Long) value);
            break;
          case FLOAT:
            byteBuffer.putFloat((Float) value);
            break;
          case INTEGER:
          case STRING:
            byteBuffer.putInt((Integer) value);
            break;
          case BOOLEAN:
            byteBuffer.put((Byte) value);
            break;
          case DOUBLE:
            byteBuffer.putDouble((Double) value);
            break;
        }
      }
      counter++;
    }
    return values.length * byteLength;
  }

  public void traverseDataInputStream(DataInputStream dis, int length, Consumer<Number> consumer) throws IOException {
    for (int entityOffset = 0; entityOffset < length; entityOffset += getByteLength()) {
      switch (this) {
        case LONG:
        case DATETIME:
          consumer.accept(dis.readLong());
          break;
        case FLOAT:
          consumer.accept(dis.readFloat());
          break;
        case INTEGER:
        case STRING:
          consumer.accept(dis.readInt());
          break;
        case BOOLEAN:
          consumer.accept(dis.readByte());
          break;
        case DOUBLE:
          consumer.accept(dis.readDouble());
          break;
      }
    }
  }
  
  /**
   * Traverses the given value byte buffer and return it in list format.
   * 
   * @param valueByteBuffer The buffer to traverse.
   * @param length The total number of bytes to traverse.
   * 
   * @return The values.
   */
  public List<Object> traverseByteBufferToList(ByteBuffer valueByteBuffer, int length) {
    List<Object> values = new ArrayList<>();
    valueByteBuffer.flip();
    for (int i = 0; i < length; i += this.getByteLength()) {
      switch (this) {
        case LONG:
        case DATETIME:
          long valueLong = valueByteBuffer.getLong();
          values.add(valueLong);
          break;
        case FLOAT:
          float valueFloat = valueByteBuffer.getFloat();
          values.add(valueFloat);
          break;
        case INTEGER:
        case STRING:
          int valueInt = valueByteBuffer.getInt();
          values.add(valueInt);
          break;
        case BOOLEAN:
          byte valueBoolean = valueByteBuffer.get();
          values.add(valueBoolean);
          break;
        case DOUBLE:
          double valueDouble = valueByteBuffer.getDouble();
          values.add(valueDouble);
          break;
      }
    }
    return values;
  }

  public void traverseByteBuffer(ByteBuffer valueByteBuffer, RoaringBitmap nilBitMap, int length, BiConsumer<Integer, Number> consumer) {
    valueByteBuffer.flip();
    int rowCount = 1;
    for (int i = 0; i < length; i += this.getByteLength()) {
      switch (this) {
        case LONG:
        case DATETIME:
          long valueLong = valueByteBuffer.getLong();
          if (nilBitMap == null || !nilBitMap.contains(rowCount))
            consumer.accept(rowCount, valueLong);
          break;
        case FLOAT:
          float valueFloat = valueByteBuffer.getFloat();
          if (nilBitMap == null || !nilBitMap.contains(rowCount))
            consumer.accept(rowCount, valueFloat);
          break;
        case INTEGER:
        case STRING:
          int valueInt = valueByteBuffer.getInt();
          if (nilBitMap == null || !nilBitMap.contains(rowCount))
            consumer.accept(rowCount, valueInt);
          break;
        case BOOLEAN:
          byte valueBoolean = valueByteBuffer.get();
          if (nilBitMap == null || !nilBitMap.contains(rowCount))
            consumer.accept(rowCount, valueBoolean);
          break;
        case DOUBLE:
          double valueDouble = valueByteBuffer.getDouble();
          if (nilBitMap == null || !nilBitMap.contains(rowCount))
            consumer.accept(rowCount, valueDouble);
          break;
      }
      rowCount++;
    }
  }

  public Number fromString(String key)
  {
    if ("null".equals(key)) {
      return null;
    } else {
      switch (this) {
        case LONG:
        case DATETIME:
          return Long.parseLong(key);
        case FLOAT:
          return Float.parseFloat(key);
        case INTEGER:
        case STRING:
          return Integer.parseInt(key);
        case BOOLEAN:
          return Byte.parseByte(key);
        case DOUBLE:
          return Double.parseDouble(key);
      }
    }
    throw new IllegalStateException("unknown object type");
  }

  public String toString(Object val) {
    if (val == null) {
      return "null";
    } else {
      switch (this) {
        case LONG:
        case DATETIME:
          return Long.toString((Long) val);
        case FLOAT:
          return Float.toString((Float) val);
        case INTEGER:
        case STRING:
          return Integer.toString((Integer) val);
        case BOOLEAN:
          return Byte.toString((Byte) val);
        case DOUBLE:
          return Double.toString((Double) val);
      }
    }
    throw new IllegalStateException("unknown object type");
  }

}
