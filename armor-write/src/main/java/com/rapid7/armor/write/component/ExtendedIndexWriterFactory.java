package com.rapid7.armor.write.component;

import com.rapid7.armor.shard.ColumnShardId;

public interface ExtendedIndexWriterFactory {
   ExtendedIndexWriter constructWriter(ColumnShardId columnShardId);
}
