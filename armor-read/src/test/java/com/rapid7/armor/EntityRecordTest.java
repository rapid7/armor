package com.rapid7.armor;

import com.rapid7.armor.entity.EntityRecord;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class EntityRecordTest {

  @Test
  public void offsetSort() {
    EntityRecord e1 = new EntityRecord(1, 0, 0, 0L, (byte) 0, 0, 0, null);
    EntityRecord e2 = new EntityRecord(2, 0, 0, 0L, (byte) 0, 0, 0, null);
    EntityRecord e3 = new EntityRecord(3, 0, 16, 0L, (byte) 0, 0, 0, null);
    EntityRecord e4 = new EntityRecord(4, 0, 0, 0L, (byte) 0, 0, 0, null);
    EntityRecord e5 = new EntityRecord(5, 0, 0, 0L, (byte) 0, 0, 0, null);
    EntityRecord e6 = new EntityRecord(0, 0, 0, 0L, (byte) 0, 0, 0, null);

    List<EntityRecord> sorted = EntityRecord.sortRecordsByOffset(Arrays.asList(e1, e2, e3, e4, e5, e6));
    assertEquals(e5.getEntityId(), sorted.get(sorted.size() - 2).getEntityId());
    assertEquals(e3.getEntityId(), sorted.get(sorted.size() - 1).getEntityId());
  }
}
