package com.rapid7.armor.util;

import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.entity.EntityRecordSummary;
import com.rapid7.armor.schema.ColumnName;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.write.component.DictionaryWriter;
import com.rapid7.armor.write.writers.ColumnFileWriter;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Compares two columns as a writer for analysis, note this should be two columns of the same type but different versions.
 */
public class ColumnWriterComparison {


  public static void main(String[] args) throws IOException {
    String column1 = "/home/alee/下載/family_S_T"; // args[0];
    String column2 = "/home/alee/下載/family_S_T"; //args[1];
    String columnType = "S"; //args[3];
    ColumnFileWriter writer1 = new ColumnFileWriter(new DataInputStream(Files.newInputStream(Paths.get(column1), StandardOpenOption.READ)),
        new ColumnShardId(new ShardId(1, "dummy", "dummy"), new ColumnName("1", columnType)));
    ColumnFileWriter writer2 = new ColumnFileWriter(new DataInputStream(Files.newInputStream(Paths.get(column2), StandardOpenOption.READ)),
        new ColumnShardId(new ShardId(1, "dummy", "dummy"), new ColumnName("2", columnType)));

    List<EntityRecordSummary> summaries1 = writer1.getEntityRecordSummaries();
    List<EntityRecordSummary> summaries2 = writer2.getEntityRecordSummaries();

    for (int i = 0; i < summaries1.size(); i++) {

      System.out.println(i + " " + summaries1.get(i));
    }

    for (int i = 0; i < summaries2.size(); i++) {
      System.out.println(i + " " + summaries2.get(i));
    }
    if (!summaries1.equals(summaries2)) {
      for (int i = 0; i < summaries1.size(); i++) {

        EntityRecordSummary ers1 = summaries1.get(i);
        EntityRecordSummary ers2 = summaries2.get(i);
        System.out.println(i + ":" + ers1.getId() + ":" + ers1.getOffset() + " " + ers2.getId() + ":" + ers2.getOffset());
        if (ers1.getId().equals("920e6463-8df5-4137-becb-c2f4ea2f3688-default-asset-111383")) {
          //System.out.println(i);
        }
        if (ers2.getId().equals("920e6463-8df5-4137-becb-c2f4ea2f3688-default-asset-111383")) {
          //System.out.println(i);
        }
        if (!ers1.equals(ers2)) {

          System.out.println("!!!!!!");
        }
      }
    }


//    for (EntityRecordSummary ers : summaries) {
//      if (ers.getId().equals("920e6463-8df5-4137-becb-c2f4ea2f3688-default-asset-111383")) {
//        System.out.println("");
//      }
//    }
    DictionaryWriter dw1 = writer1.getEntityDictionary();
    Set<Object> ee = dw1.isCorrupted();
    DictionaryWriter dw2 = writer2.getEntityDictionary();
    Set<Object> ff = dw2.isCorrupted();

    Integer test1 = dw1.getSurrogate("920e6463-8df5-4137-becb-c2f4ea2f3688-default-asset-111383");
    String value1 = dw1.getValue(test1);
    Integer test3 = dw1.getSurrogate("920e6463-8df5-4137-becb-c2f4ea2f3688-default-asset-154162");
    String value2 = dw1.getValue(test3);

    for (Integer v1 : dw2.getStrToInt().values()) {
      if (v1.equals(test1)) {
        System.out.println("Got test1");
      }
      if (v1.equals(test3)) {
        System.out.println("Got test3");
      }
    }


    Integer test2 = dw2.getSurrogate("920e6463-8df5-4137-becb-c2f4ea2f3688-default-asset-111383");
    String value3 = dw1.getValue(test2);

    Integer test4 = dw2.getSurrogate("920e6463-8df5-4137-becb-c2f4ea2f3688-default-asset-154162");
    String value4 = dw1.getValue(test4);

    for (Integer v1 : dw2.getStrToInt().values()) {
      if (v1.equals(test2)) {
        System.out.println("Got test2");
      }
      if (v1.equals(test4)) {
        System.out.println("Got test4");
      }
    }

    if (dw1 != null && dw2 != null) {
      System.out.println("Analyzing dictionary for any changes");
      for (Map.Entry<String, Integer> entry : dw1.getStrToInt().entrySet()) {
        String value = entry.getKey();
        Integer surrogate = entry.getValue();
        Integer otherSurrogate = dw2.getSurrogate(value);
        if (otherSurrogate == null) {
          System.out.println("The value " + value + " was removed from 2nd column");
        } else if (surrogate.intValue() != otherSurrogate.intValue()) {
          System.out.println("The value " + value + " surrogate was changed from " + surrogate + " to " + otherSurrogate);
        }
      }

      // Do a check the other way now
      Set<String> differences = dw2.getStrToInt().keySet().stream()
          .filter(key -> !dw1.getStrToInt().containsKey(key))
          .collect(Collectors.toSet());
      for (String difference2 : differences) {
        System.out.println("The value " + difference2 + " is new in the 2nd colunmn");
      }
    } else {
      if (dw1 == null && dw2 == null)
        System.out.println("No dictionary analysis can be applied");
      else
        System.out.println("One of the columns dictionaries doesn't exist");
    }

    System.out.println("Executing entity record analysis");
    Map<Integer, EntityRecord> entityRecords1 = writer1.getEntites();
    Map<Integer, EntityRecord> entityRecords2 = writer2.getEntites();

    // With records analysis we will now be looking at the changes, meaning records that have changed from 1 vs 2.
    Set<EntityRecord> changes2 = new HashSet<>();
    Set<EntityRecord> removed = new HashSet<>();
    Set<EntityRecord> added = new HashSet<>();

    for (EntityRecord entityRecord1 : entityRecords1.values()) {
      EntityRecord entityRecord2 = entityRecords2.get(entityRecord1.getEntityId());
      if (entityRecord2 != null) {
        if (entityRecord1.getRowGroupOffset() == entityRecord2.getRowGroupOffset()) {
          continue;
        }
        changes2.add(entityRecord2);
      } else {
        removed.add(entityRecord1);
      }
    }

    Set<Integer> erDiff = entityRecords2.keySet().stream()
        .filter(key -> !entityRecords1.containsKey(key))
        .collect(Collectors.toSet());
    for (Integer id2 : erDiff) {
      added.add(entityRecords2.get(id2));
    }

    System.out.println("Here are the entities that were removed");
    for (EntityRecord removedRecord : removed) {
      System.out.println(removedRecord);
    }
    System.out.println("Here are the entities that were added " + added.size() + " were added");
    for (EntityRecord add : added) {
      System.out.println(add);
    }
    System.out.println("Here are the entities that were added");
    for (EntityRecord change2 : changes2) {
      EntityRecord change1 = entityRecords1.get(change2.getEntityId());
      System.out.println("Changed from " + change1.getEntityId() + ":" + change1.getRowGroupOffset() + " to " + change2.getEntityId() + ":" + change2.getRowGroupOffset());
    }
    System.out.println("Finished analysis");
  }
}
