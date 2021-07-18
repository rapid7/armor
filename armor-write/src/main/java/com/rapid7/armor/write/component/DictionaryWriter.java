package com.rapid7.armor.write.component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rapid7.armor.dictionary.Dictionary;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class DictionaryWriter implements Component, Dictionary {
  private final AtomicInteger nextInteger;
  private Map<String, Integer> strToInt = new HashMap<>();
  private final Map<Integer, String> intToStr = new HashMap<>();
  private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private boolean bidirectional = false;
  
  public DictionaryWriter(boolean bidirectional) {
    nextInteger = new AtomicInteger(1);
    this.bidirectional = bidirectional;
  }

  public DictionaryWriter(byte[] json, boolean bidirectional) throws IOException {
    this.bidirectional = bidirectional;
    @SuppressWarnings("unchecked")
    Map<Object, String> map = OBJECT_MAPPER.readValue(json, Map.class);
    strToInt = new HashMap<>();
    int highestSurrogate = -1;
    for (Map.Entry<Object, String> e : map.entrySet()) {
      Integer surrogate = Integer.parseInt(e.getKey().toString());
      if (highestSurrogate < surrogate)
        highestSurrogate = surrogate;
      strToInt.put(e.getValue(), surrogate);
      if (bidirectional)
        intToStr.put(surrogate, e.getValue());
    }
    if (highestSurrogate > 0)
      nextInteger = new AtomicInteger(highestSurrogate);
    else
      nextInteger = new AtomicInteger(1);
  }

  public boolean isEmpty() {
    return strToInt.isEmpty();
  }

  public Map<Integer, String> getIntToStr() {
    return intToStr;
  }

  public Map<String, Integer> getStrToInt() {
    return strToInt;
  }

  @Override
  public InputStream getInputStream() throws IOException {
    HashMap<Integer, String> reverse = new HashMap<>();
    for (Map.Entry<String, Integer> e : strToInt.entrySet()) {
      reverse.put(e.getValue(), e.getKey());
    }
    return new ByteArrayInputStream(OBJECT_MAPPER.writeValueAsBytes(reverse));
  }

  @Override
  public long getCurrentSize() throws IOException {
    HashMap<Integer, String> reverse = new HashMap<>();
    for (Map.Entry<String, Integer> e : strToInt.entrySet()) {
      reverse.put(e.getValue(), e.getKey());
    }
    return OBJECT_MAPPER.writeValueAsBytes(reverse).length;
  }
  
  public synchronized void removeValue(Integer surrogate) {
    String removedValue = intToStr.remove(surrogate);
    if (removedValue != null)
      strToInt.remove(removedValue);
  }

  public synchronized void removeSurrogate(String value) {
    Integer removedSurrogate = strToInt.remove(value);
    if (removedSurrogate != null)
      intToStr.remove(removedSurrogate);
  }

  public Set<Object> isCorrupted() {
    Set<Integer> foundInts = new HashSet<>();
    Set<Object> duplicates = new HashSet<>();
    for (Integer value : strToInt.values()) {
      if (foundInts.contains(value)) {
        duplicates.add(value);
      } else
        foundInts.add(value);
    }
    Set<String> foundStr = new HashSet<>();
    for (String value : intToStr.values()) {
      if (foundStr.contains(value)) {
        duplicates.add(value);
      } else
        foundStr.add(value);
    }
    return duplicates;
  }

  public String getValue(Integer surrogate) {
    if (!bidirectional)
      throw new IllegalStateException("This dictionary is not setup for bidirectional");
    return intToStr.get(surrogate);
  }

  public int cardinality() {
    return strToInt.size();
  }

  public boolean contains(Integer key) {
    if (!bidirectional)
      throw new IllegalStateException("This dictionary is not setup for bidirectional");
    return intToStr.containsKey(key);
  }

  public boolean contains(String key) {
    return strToInt.containsKey(key);
  }

  public synchronized Integer getSurrogate(String value) {
    Integer toReturn = null;
    if (!strToInt.containsKey(value)) {
      int nextVal = nextInteger.incrementAndGet();
      strToInt.put(value, nextVal);
      toReturn = nextVal;
      if (bidirectional)
        intToStr.put(toReturn, value);
    } else {
      toReturn = strToInt.get(value);
    }
    return toReturn;
  }
}
