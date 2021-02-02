package com.rapid7.armor.schema;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestColumnName {

  @Test
  public void columnName() {
    ColumnName cn = new ColumnName("asset_I");
    assertEquals("asset", cn.getName());
    assertEquals("I", cn.getType());
    
    ColumnName cn1 = new ColumnName("asset_I_A");
    assertEquals("asset", cn1.getName());
    assertEquals("I", cn1.getType());
    
    ColumnName cn3 = new ColumnName("asset_I_A88888_333_asset_S");
    assertEquals("asset", cn3.getName());
    assertEquals("I", cn3.getType());
  }
}
