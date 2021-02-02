package com.rapid7.armor;

import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.entity.EntityRecordSummary;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.read.FastArmorShard;
import com.rapid7.armor.schema.ColumnName;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.write.ColumnWriter;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;

public class SpecialDriverRead2 {

  public static void main(String[] args) throws IOException {
    FastArmorShard fas = new FastArmorShard(Files.newInputStream(Paths.get("/home/alee/下載/assetId_I_A"), StandardOpenOption.READ));

    ColumnWriter writer = new ColumnWriter(new DataInputStream(Files.newInputStream(Paths.get("/home/alee/下載/swId_I"), StandardOpenOption.READ)),
        new ColumnShardId(new ShardId(1, "myorg", "table"), new ColumnName("dd", "I")));
    Map<Integer, EntityRecord> baseLineRecords = writer.getEntites();
    List<EntityRecordSummary> baselineSummaries = writer.getEntityRecordSummaries();

    ColumnWriter writer1 = new ColumnWriter(new DataInputStream(Files.newInputStream(Paths.get("/home/alee/下載/v1"), StandardOpenOption.READ)),
        new ColumnShardId(new ShardId(1, "myorg", "table"), new ColumnName("dd", "S")));
    List<EntityRecordSummary> testSummaries = writer1.getEntityRecordSummaries();
    for (int i = 0; i < baselineSummaries.size(); i++) {
      System.out.println(baselineSummaries.get(i).getId() + " vs " + testSummaries.get(i).getId());
      System.out.println(baselineSummaries.get(i).getNumRows() + " vs " + testSummaries.get(i).getNumRows());

      if (!baselineSummaries.get(i).getId().equals(testSummaries.get(i).getId())) {
        System.out.println();
      }
    }
    List<EntityRecord> records = EntityRecord.sortRecordsByOffset(baseLineRecords.values());
    for (EntityRecord er : records) {
      System.out.println(er.getEntityId() + "_" + er.getRowGroupOffset() + "_" + er.getValueLength() + "_" + er.totalLength());
    }
    ColumnMetadata data = writer.getMetadata();
    System.out.println("dd");
  }
}
