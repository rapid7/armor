package com.rapid7.armor.write;

import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.shard.ColumnShardId;
import java.util.List;

public class EntityOffsetException extends RuntimeException {
  private static final long serialVersionUID = 5901323390451574236L;
  private final ColumnShardId columnShardId;
  private final int cursor;
  private final EntityRecord recordError;
  private final List<EntityRecord> entities;

  public EntityOffsetException(ColumnShardId columnShardId, int cursor, EntityRecord recordError, List<EntityRecord> entities) {
    this.columnShardId = columnShardId;
    this.cursor = cursor;
    this.recordError = recordError;
    this.entities = entities;
  }

  @Override
  public String getMessage() {
    // Build up the a display to show all the records before using
    StringBuilder sb = new StringBuilder();
    for (EntityRecord er : entities) {
      sb.append(er.getEntityId());
      sb.append("_");
      sb.append(er.getRowGroupOffset());
      sb.append(":");
      sb.append(er.getValueLength());
      sb.append(":");
      sb.append(er.getNullLength());
      sb.append("\n");
    }
    return "On column " + columnShardId.toSimpleString() + " detected offset issue at " + cursor + " of " + recordError.getEntityId() + " here are the records:\n" + sb.toString();
  }
}
