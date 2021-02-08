package com.rapid7.armor.schema;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestColumnIds {

  @Test
  public void columnIds() {
    ColumnId cn = new ColumnId("asset_I");
    assertEquals("asset", cn.getName());
    assertEquals("I", cn.getType());
    
    ColumnId cn1 = new ColumnId("asset_I_A");
    assertEquals("asset", cn1.getName());
    assertEquals("I", cn1.getType());
    
    ColumnId cn3 = new ColumnId("asset_I_A88888_333_asset_S");
    assertEquals("asset", cn3.getName());
    assertEquals("I", cn3.getType());
  }
}
