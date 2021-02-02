package com.rapid7.armor;

import com.rapid7.armor.read.FastArmorBlock;
import com.rapid7.armor.read.SlowArmorReader;
import com.rapid7.armor.read.FastArmorColumnReader;
import com.rapid7.armor.read.FastArmorReader;
import com.rapid7.armor.shard.ModShardStrategy;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.FileReadStore;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import tech.tablesaw.columns.Column;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class SpecialDriverRead {

  public static void main(String[] args) throws IOException {
    ModShardStrategy mss = new ModShardStrategy(10);
    FileReadStore fileStore = new FileReadStore(Paths.get("/home/alee/nike/armor"));
    Instant start = Instant.now();
    List<ShardId> shardIds = fileStore.findShardIds("testorg1", "asset_sw");
    assertFalse(shardIds.isEmpty());


    SlowArmorReader armorReader1 = new SlowArmorReader(fileStore);
    Column<?> test = armorReader1.getColumn("testorg1", "asset_sw", "assetId", 9);
    System.out.println(test.countUnique());
    Set<Object> tt2 = Arrays.stream(test.asObjectArray()).collect(Collectors.toSet());
    System.out.println(tt2.size());

    FastArmorReader armorReader = new FastArmorReader(fileStore);
    Instant mark1 = Instant.now();
    FastArmorColumnReader fastReader = armorReader.getColumn("testorg2", "asset_sw", "version", 0);
    FastArmorBlock fb = fastReader.getStringBlock(100);
    //fastReader.hasNext();
    //int[] ee = armorReader.getEntitiesColumn("testorg1", "asset_sw", "product", 0);

    System.out.println("Took " + Duration.between(mark1, Instant.now()) + " ");//tt.length);
  }
}
