package com.rapid7.armor.util;

import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.entity.EntityRecordSummary;
import com.rapid7.armor.schema.ColumnName;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.write.ColumnFileWriter;
import com.rapid7.armor.write.WriteRequest;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Compares two columns as a writer for analysis, note this should be two columns of the same type but different versions.
 */
public class ColumnWriteFromJson {
  public static void main(String[] args) throws IOException {
    String column1 = "/home/alee/下載/family_S_2"; // args[0];
    //String column1 = "/home/alee/test/myorg/asset_sw_legacy/1/test/product_S"; // args[0];

    String writeRequestPath = "/home/alee/下載/writeRequests.txt"; //args[1];
    String columnType = "S"; //args[3];
    //FileWriteStore fws = new FileWriteStore(Paths.get("/home/alee/test"), new ModShardStrategy(1));
    ColumnFileWriter writer1 = new ColumnFileWriter(new DataInputStream(Files.newInputStream(Paths.get(column1), StandardOpenOption.READ)),
        new ColumnShardId(new ShardId(1, "dummy", "dummy"), new ColumnName("product", columnType)));
    List<EntityRecordSummary> summaries1 = writer1.getEntityRecordSummaries();
    Integer test = writer1.getEntityDictionary().getSurrogate("97039950-049c-4589-9ab4-53d8d5cc9558-default-asset-2286607");
    Integer test2 = writer1.getEntityDictionary().getSurrogate("920e6463-8df5-4137-becb-c2f4ea2f3688-default-asset-441035");
//    writer1.getEntityDictionary().removeSurrogate("920e6463-8df5-4137-becb-c2f4ea2f3688-default-asset-441035");
//    StreamProduct streamProduct = writer1.buildInputStream();

//    try (InputStream inputStream = streamProduct.getInputStream()) {
//      fws.saveColumn("test", new ColumnShardId(new ShardId(1, "myorg", "asset_sw_legacy"), new ColumnName("product", columnType)), streamProduct.getByteSize(), inputStream);
//    }
    Map<Integer, EntityRecord> dd = writer1.getEntites();
    EntityRecord er = dd.get(test);
    Set<Object> corrupted = writer1.getEntityDictionary().isCorrupted();
    for (int i = 0; i < summaries1.size(); i++) {
      if (summaries1.get(i).getId().equals("97039950-049c-4589-9ab4-53d8d5cc9558-default-asset-2286607"))
        System.out.println();
      System.out.println(i + " " + summaries1.get(i));
    }


    ObjectMapper om = new ObjectMapper();
    byte[] payload = Files.readAllBytes(Paths.get(writeRequestPath));
    List<WriteRequest> writeRequests = om.readValue(payload, new TypeReference<List<WriteRequest>>() {
    });
    writer1.write("test", writeRequests);

    List<EntityRecordSummary> summaries2 = writer1.getEntityRecordSummaries();
    for (int i = 0; i < summaries2.size(); i++) {
      if (summaries2.get(i).getId().equals("920e6463-8df5-4137-becb-c2f4ea2f3688-default-asset-111383"))
        System.out.println();
      System.out.println(i + " " + summaries2.get(i));
    }

    System.out.println();

  }
}
