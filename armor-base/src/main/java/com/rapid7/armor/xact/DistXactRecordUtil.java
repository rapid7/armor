package com.rapid7.armor.xact;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rapid7.armor.io.PathBuilder;

public class DistXactRecordUtil {
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static String CURRENT = "current";
  private static String CURRENT_TIME = "currentTime";
  private static String PREVIOUS = "previous";
  private static String PREVIOUS_TIME = "previousTime";
  private static String CURRENT_AUTO = "currentAuto";

  @SuppressWarnings("unchecked")
  public static DistXactRecord readXactRecord(InputStream is) {
    try {
      Map<String, Object> status = OBJECT_MAPPER.readValue(is, Map.class);
      if (status != null) {
        DistXactRecord record = new DistXactRecord(status.get(CURRENT), status.get(CURRENT_TIME), status.get(PREVIOUS), status.get(PREVIOUS_TIME));
        if (status.containsKey(CURRENT_AUTO))
            record.setAutoCurrent(Boolean.parseBoolean(status.get(CURRENT_AUTO).toString()));
        return record;
      }
      return null;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
    
  public static String buildCurrentMarker(String path) {
    return PathBuilder.buildPath(path, DistXactRecord.CURRENT_MARKER);
  }
  
  public static String prepareToCommit(DistXactRecord record) {
    try {
      HashMap<String, Object> currentValues = new HashMap<>();
      currentValues.put(CURRENT, record.getCurrent());
      currentValues.put(CURRENT_TIME, record.getCurrentTime());
      if (record.getPrevious() != null) {
        currentValues.put(PREVIOUS, record.getPrevious());
        currentValues.put(PREVIOUS_TIME, record.getPreviousTime());
      }
      if (record.getAutoCurrent()) {
        currentValues.put(CURRENT_AUTO, record.getAutoCurrent());
      }
      String payload = OBJECT_MAPPER.writeValueAsString(currentValues);
      return payload;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
  
  public static void validateXact(DistXactRecord baseline, ArmorXact armorTransaction) {    
    // Ensure the begin doesn't match any previous transactions we know about.
    if (baseline.getCurrent().equalsIgnoreCase(armorTransaction.getTarget()))
      throw new XactError(armorTransaction, baseline);
    if (baseline.getPrevious() != null && baseline.getPrevious().equalsIgnoreCase(armorTransaction.getTarget()))
      throw new XactError(armorTransaction, baseline);
      
    // Check the baseline MUST match current
    if (!baseline.getCurrent().equalsIgnoreCase(armorTransaction.getBaseline()))
      throw new XactError(armorTransaction, baseline);
  }
}
