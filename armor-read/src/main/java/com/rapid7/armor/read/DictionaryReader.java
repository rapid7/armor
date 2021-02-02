package com.rapid7.armor.read;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DictionaryReader {
  private final Map<Integer, byte[]> intToBytes;
  private Map<String, Integer> strToInt;
  private int valueLength;
  private final boolean bidirectional = false;

  public DictionaryReader(String json, int initialCapacity, boolean bidirectional) throws IOException {
    @SuppressWarnings("unchecked")
    Map<String, String> map = new ObjectMapper().readValue(json.getBytes(), Map.class);
    // Since integers are stored as string, convert them to real ints.
    intToBytes = new HashMap<>(initialCapacity);
    for (Map.Entry<String, String> e : map.entrySet()) {
      int surrogate = Integer.parseInt(e.getKey());
      if (intToBytes.containsKey(surrogate))
        throw new RuntimeException();
      intToBytes.put(surrogate, e.getValue().getBytes());
      if (bidirectional) {
        strToInt = new HashMap<>(initialCapacity);
        if (strToInt.containsKey(e.getValue()))
          throw new RuntimeException();
        strToInt.put(e.getValue(), surrogate);
      }
    }
    intToBytes.put(0, "".getBytes());
  }

  public int test() {
    return valueLength;
  }

  public Integer getSurrogate(String value) {
    if (bidirectional)
      return strToInt.get(value);
    throw new UnsupportedOperationException();
  }

  public String getValueAsString(Integer surrogate) {
    return new String(intToBytes.get(surrogate));
  }

  public byte[] getValue(Integer surrogate) {
    return intToBytes.get(surrogate);
  }
}
