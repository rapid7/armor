package com.rapid7.armor.xact;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DistXactUtil {
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @SuppressWarnings("unchecked")
  public static DistXact readXactStatus(InputStream is) {
    try {
      Map<String, String> status = OBJECT_MAPPER.readValue(is, Map.class);
      if (status != null) {
        return new DistXact(status.get("current"), status.get("previous"));
      }
      return null;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
  
  public static String buildCurrentMarker(String path, String delimeter) {
    if (delimeter == null)
      return path + "/" + DistXact.CURRENT_MARKER;
    else
      return path + delimeter + DistXact.CURRENT_MARKER;
  }
  
  
  public static String buildCurrentMarker(String path) {
    return path + "/" + DistXact.CURRENT_MARKER;
  }
  
  public static String prepareToCommit(DistXact status) {
    try {
      HashMap<String, String> currentValues = new HashMap<>();
      currentValues.put("current", status.getCurrent());
      if (status.getPrevious() != null)
        currentValues.put("previous", status.getPrevious());
      String payload = OBJECT_MAPPER.writeValueAsString(currentValues);
      return payload;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
