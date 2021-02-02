package com.rapid7.armor.write;

import com.rapid7.armor.entity.Column;
import com.rapid7.armor.schema.ColumnName;
import com.rapid7.armor.schema.DataType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SerDeTest {

  @Test
  public void columnNameSerDer() throws IOException {
    ObjectMapper om = new ObjectMapper();
    WriteRequest icpr = new WriteRequest();
    Column c = new Column(new ColumnName("a", DataType.STRING.getCode()));
    c.addValue(null);
    c.addValue("test");
    icpr.setColumn(c);
    icpr.setEntityId(10);
    icpr.setVersion(333L);
    String icprStr = om.writeValueAsString(icpr);
    WriteRequest testIcpr = om.readValue(icprStr, WriteRequest.class);
    assertEquals(icpr, testIcpr);
  }
}
