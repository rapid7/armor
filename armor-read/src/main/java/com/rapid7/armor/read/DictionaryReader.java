package com.rapid7.armor.read;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rapid7.armor.read.predicate.StringPredicate;
import com.rapid7.armor.store.Operator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DictionaryReader {
  private final Map<Integer, byte[]> intToBytes;
  private Map<String, Integer> strToInt;
  private boolean bidirectional = false;
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public DictionaryReader(byte[] json, int capacity, boolean bidirectional) throws IOException {
    this.bidirectional = bidirectional;
    if (bidirectional)
        strToInt = new HashMap<>(capacity);
    @SuppressWarnings("unchecked")
    Map<String, String> map = OBJECT_MAPPER.readValue(json, Map.class);
    // Since integers are stored as string, convert them to real ints.
    intToBytes = new HashMap<>(capacity);
    for (Map.Entry<String, String> e : map.entrySet()) {
      int surrogate = Integer.parseInt(e.getKey());
      if (intToBytes.containsKey(surrogate)) {
        throw new RuntimeException("The surrogate " + surrogate + " already contains value " + new String(intToBytes.get(surrogate)) + " cannot add " + e.getValue());
      }
      intToBytes.put(surrogate, e.getValue().getBytes());
      if (bidirectional) {
        if (strToInt.containsKey(e.getValue()))
          throw new RuntimeException("The value " + e.getValue() + " already contains a surrogate " + strToInt.get(e.getValue()) + " cannot add " + e.getValue());
        strToInt.put(e.getValue(), surrogate);
      }
    }
    intToBytes.put(0, "".getBytes());
  }

  /**
   * Evaluates the predicate, will return true, false or null. If null
   * then the predicate couldn't be evaluated.
   *
   * @param predicate The predicate to evaluate.
   *
   * @return true/false and null. Null denotes predicate doesn't apply.
   */
  public Boolean evaulatePredicate(StringPredicate predicate) {
    if (strToInt == null) {
        throw new RuntimeException("The strToInt cannot be null if you want to evalue predicate. Set bidirectional to true");
    }
    if (predicate.getOperator() == Operator.IS_NULL) {
        return null;
    }
    if (predicate.getOperator() == Operator.NOT_NULL) {
        return !strToInt.isEmpty();
    }
    if (predicate.getOperator() == Operator.EQUALS) {
        return strToInt.containsKey(predicate.getValue());
    } else if (predicate.getOperator() == Operator.NOT_EQUALS) {
        return !strToInt.containsKey(predicate.getValue());
    } else {
        for (String value : strToInt.keySet()) {
            if (predicate.test(value))
                return true;
        }
    }
    return false;
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
