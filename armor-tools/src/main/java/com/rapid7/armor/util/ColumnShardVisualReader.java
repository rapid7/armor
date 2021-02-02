package com.rapid7.armor.util;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.roaringbitmap.RoaringBitmap;

import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.schema.ColumnName;
import com.rapid7.armor.write.ColumnWriter;

/**
 * Prints out the column shard contents into a visually more intuitive format. The format does not
 * filter any values it is supposed to show the information as to how its stored on disk.
 * 
 * 0:[id=1,del=1,instanceid=eeee,version=1][val1, val2, val3][] => [val1, val2, val3]
 * 10:199 DEADSPACE
 * 200:[id=2,del=0,instanceid=dddd,version=1][val1, val2, val3][0] => [null, val2, val3]
 * 
 * The above first row has no null values. The 2nd row the first entry has a null value.
 */
public class ColumnShardVisualReader {
  private Path target = null;
  public ColumnShardVisualReader() {}
  public ColumnShardVisualReader(ColumnName columnName, Path target) {
    this.target = target.resolve(columnName.getName() + "-column-data");
  }

  public void process(ColumnWriter columnWriter) throws IOException {
    List<EntityRecord> allRecords = columnWriter.allEntityRecords();
    final FileOutputStream faos; 
    if (target != null) {
      faos = new FileOutputStream(target.toFile());
    } else
      faos = null;
    try {
      columnWriter.getRowGroupWriter().customTraverseThoughValues(allRecords, (a) -> {
        boolean isDeadSpace = (Boolean) a.get(0);
        StringBuilder sb = new StringBuilder();
        if (isDeadSpace) {
          int start = (Integer) a.get(1);
          int end = (Integer) a.get(2);
          sb.append(start);
          sb.append(":");
          sb.append(end);
          sb.append(" is DEADSPACE");
        } else {
          EntityRecord er = (EntityRecord) a.get(1);
          List<Object> values = (List<Object>) a.get(2);
          RoaringBitmap roarBitMap = (RoaringBitmap) a.get(3);
          
          sb.append(er.getRowGroupOffset());
          sb.append(":[id=");
          sb.append(er.getEntityId());
          sb.append(",");
          sb.append(",del=");
          sb.append(er.getDeleted());
          sb.append(",version=");
          sb.append(er.getVersion());
          sb.append(",instanceid=");
          sb.append(new String(er.getInstanceId()));
          sb.append("]");
          sb.append(values.toString());
          if (roarBitMap == null)
            sb.append("[]");
          else
            sb.append(roarBitMap.toArray());
          sb.append(" ~> ");
          if (roarBitMap == null) {
            sb.append(values.toString());
          } else {
            for (int row : roarBitMap.toArray()) {
              values.set(row-1, null);
            }
            sb.append(values.toString());
          }
        }
        sb.append("\n");
        // Now check the target if not path, then simply print out to console
        if (faos == null)
          System.out.print(sb.toString());
        else {
          try {
            faos.write(sb.toString().getBytes());
          } catch (IOException ioe) {
            throw new RuntimeException(ioe);
          }
        }
      });
    } finally {
      if (faos != null)
        faos.close();
    }
  }
}
