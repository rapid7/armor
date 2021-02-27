package com.rapid7.armor.io;

import com.rapid7.armor.Constants;

public class PathBuilder {

  public static String buildPath(String... parts) {
    StringBuilder sb = new StringBuilder();
    for (String part : parts) {
      sb.append(part);
      sb.append(Constants.STORE_DELIMETER);
    }
    return sb.substring(0, sb.length()-1);
  }
}
