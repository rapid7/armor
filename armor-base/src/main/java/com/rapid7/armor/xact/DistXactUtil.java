package com.rapid7.armor.xact;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rapid7.armor.io.PathBuilder;

public class DistXactUtil {
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @SuppressWarnings("unchecked")
  public static DistXact readXactStatus(InputStream is) {
    try {
      Map<String, Object> status = OBJECT_MAPPER.readValue(is, Map.class);
      if (status != null) {
        return new DistXact(status.get("current"), status.get("currentTime"), status.get("previous"), status.get("previousTime"));
      }
      return null;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
    
  public static String buildCurrentMarker(String path) {
    return PathBuilder.buildPath(path, DistXact.CURRENT_MARKER);
  }
  
  public static String prepareToCommit(DistXact status) {
    try {
      HashMap<String, Object> currentValues = new HashMap<>();
      currentValues.put("current", status.getCurrent());
      currentValues.put("currentTime", status.getCurrentTime());
      if (status.getPrevious() != null) {
        currentValues.put("previous", status.getPrevious());
        currentValues.put("previousTime", status.getPreviousTime());
      }
      String payload = OBJECT_MAPPER.writeValueAsString(currentValues);
      return payload;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
