package com.rapid7.armor;

import com.rapid7.armor.entity.Entity;
import com.rapid7.armor.schema.ColumnName;
import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ModShardStrategy;
import com.rapid7.armor.store.FileWriteStore;
import com.rapid7.armor.write.ArmorWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BuildSmallTableWithNulls {
  private static List<ColumnName> buildColumns() {
    return Arrays.asList(
        new ColumnName("family", DataType.STRING.getCode()),
        new ColumnName("vendor", DataType.STRING.getCode()),
        new ColumnName("product", DataType.STRING.getCode()),
        new ColumnName("version", DataType.STRING.getCode()));
  }

  public static void main(String[] args) throws IOException {
    ModShardStrategy test = new ModShardStrategy(1);
    FileWriteStore fileStore = new FileWriteStore(Paths.get("/home/alee/nike/armor"), test);
    try (ArmorWriter aw = new ArmorWriter("nulls-org", fileStore, 1, false, null, null)) {
      String transaction = aw.startTransaction();
      Entity entity = Entity.buildEntity("assetId", 1, System.currentTimeMillis(), null, buildColumns());
      entity.addRow("family", null, "product", null);
      aw.write(transaction, "nulls-org", "small", Collections.singletonList(entity));
      aw.save(transaction, "nulls-org", "small");
    }
  }

  public static class Asset {
    private String assetId;
    private final List<Software> software = new ArrayList<>();
  }

  public static class Software {
    private String family;
    private String vendor;
    private String product;
    private String version;
  }
}
