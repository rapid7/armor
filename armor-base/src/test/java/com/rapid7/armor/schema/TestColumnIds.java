package com.rapid7.armor.schema;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestColumnIds {

  @Test
  public void columnIds() {
    ColumnId cn = new ColumnId("asset_I");
    assertEquals("asset", cn.getName());
    assertEquals("I", cn.getType());
    
    ColumnId cn1 = new ColumnId("asset_A_I");
    assertEquals("asset_A", cn1.getName());
    assertEquals("I", cn1.getType());
    
    ColumnId cn3 = new ColumnId("asset_I_A88888_333_asset_S");
    assertEquals("asset_I_A88888_333_asset", cn3.getName());
    assertEquals("S", cn3.getType());
  }
}
