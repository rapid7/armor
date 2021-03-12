package com.rapid7.armor.write.writers;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.write.WriteRequest;

public interface IShardWriter extends AutoCloseable {
  public ShardMetadata commit(String transaction, ColumnId columnEntityId) throws IOException;
  public ShardId getShardId();
  public void delete(String transaction, Object entityId, long version, String instanceId);
  public ColumnMetadata getMetadata(String columnId);
  public void write(String transaction, ColumnId columnId, List<WriteRequest> columns) throws IOException;
  public Map<Integer, EntityRecord> getEntities(String columnId);

}
