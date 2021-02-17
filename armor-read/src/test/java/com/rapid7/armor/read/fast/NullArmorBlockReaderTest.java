package com.rapid7.armor.read.fast;

import org.junit.jupiter.api.Test;

import io.airlift.slice.Slice;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NullArmorBlockReaderTest {

  @Test
  public void verifyNullString() {
    NullArmorBlockReader nabr1 = new NullArmorBlockReader(100);
    FastArmorBlock fab1 = nabr1.getStringBlock(200);
    Slice slice1 = fab1.getSlice();
    assertEquals(0, slice1.length());
    assertEquals(100, fab1.getNumRows());
    assertEquals(100, fab1.getValuesIsNull().length);
    verifyAllNull(fab1.getValuesIsNull());
    assertEquals(101, fab1.getOffsets().length); // Remember should include last offset.
    verifyAllZero(fab1.getOffsets());
    assertEquals(false, nabr1.hasNext());
    assertEquals(1, nabr1.batchNum());

    NullArmorBlockReader nabr2 = new NullArmorBlockReader(100);
    int batch = 1;
    for (int i = 0; i < 100; i += 10) {
      FastArmorBlock fab2 = nabr2.getStringBlock(10);
      Slice slice2 = fab1.getSlice();
      assertEquals(0, slice2.length());
      assertEquals(10, fab2.getNumRows());
      assertEquals(10, fab2.getValuesIsNull().length);
      assertEquals(11, fab2.getOffsets().length);
      verifyAllNull(fab2.getValuesIsNull());
      verifyAllZero(fab2.getOffsets());
      assertEquals(i + 10 < 100, nabr2.hasNext());
      assertEquals(batch, nabr2.batchNum());
      batch++;
    }
  }
  
  private void verifyAllZero(int[] intArray) {
    for (int i = 0; i < intArray.length; i++)
      assertEquals(0, intArray[i]);
  }
  private void verifyAllNull(boolean[] byteArray) {
    for (int i = 0; i < byteArray.length; i++)
      assertTrue(byteArray[i]);
  }
}
