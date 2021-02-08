package com.rapid7.armor.write.writers;

import com.rapid7.armor.entity.Column;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.entity.EntityRecordSummary;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.WriteStore;
import com.rapid7.armor.write.StreamProduct;
import com.rapid7.armor.write.WriteRequest;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles writes for one or more columns for a shard in one "atomic" operation.
 */
public class ShardWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(ShardWriter.class);

  private Map<ColumnShardId, ColumnFileWriter> columnFileWriters = new HashMap<>();
  private final WriteStore store;
  private final ShardId shardId;
  private final BiPredicate<ShardId, String> captureWrite;
  private final Supplier<Integer> defragTrigger;
  private boolean compress = true;
  
  public ShardWriter(
      ShardId shardId,
      WriteStore store,
      boolean compress,
      Supplier<Integer> defragTriggerSupplier,
      BiPredicate<ShardId, String> captureWrite) {
    this.shardId = shardId;
    this.store = store;
    this.compress = compress;
    if (defragTriggerSupplier == null)
      this.defragTrigger = () -> 90;
    else
      this.defragTrigger = defragTriggerSupplier;
    this.captureWrite = captureWrite;
    // Load all columns
    List<ColumnFileWriter> columnWriters = store.loadColumnWriters(shardId.getTenant(), shardId.getTable(), shardId.getShardNum());
    columnFileWriters = columnWriters.stream().collect(Collectors.toMap(ColumnFileWriter::getColumnShardId, w -> w));
  }

  public void close() {
    for (ColumnFileWriter writer : columnFileWriters.values()) {
      try {
        writer.close();
      } catch (Exception e) {
        LOGGER.warn("Unable to close column {}", writer.getColumnId(), e);
      }
    }
  }

  public ShardId getShardId() {
    return shardId;
  }

  private ColumnFileWriter getWriterByColumnId(String columnId) {
    for (ColumnFileWriter writer : columnFileWriters.values()) {
      if (writer.getColumnId().getName().equals(columnId))
        return writer;
    }
    return null;
  }

  public Map<Integer, EntityRecord> getEntities(String columnId) {
    ColumnFileWriter csw = getWriterByColumnId(columnId);
    if (csw != null)
      return csw.getEntites();
    return null;
  }

  public ColumnMetadata getMetadata(String columnId) {
    ColumnFileWriter csw = getWriterByColumnId(columnId);
    if (csw != null)
      return csw.getMetadata();
    return null;
  }

  public ShardMetadata commit(String transaction, String entityIdColumn, DataType entityIdType) throws IOException {
    boolean committed = false;
    try {
      ColumnMetadata entityColumnMetadata = consistencyCheck(transaction, entityIdColumn, entityIdType);
      for (Map.Entry<ColumnShardId, ColumnFileWriter> entry : columnFileWriters.entrySet()) {
        StreamProduct streamProduct = entry.getValue().buildInputStream(compress);
        try (InputStream inputStream = streamProduct.getInputStream()) {
          store.saveColumn(transaction, entry.getKey(), streamProduct.getByteSize(), inputStream);
        }
      }

      // Do this after the save, to ensure metadata is updated.
      List<ColumnMetadata> columnMetadata = columnFileWriters.values().stream().map(ColumnFileWriter::getMetadata).collect(Collectors.toList());
      columnMetadata.add(entityColumnMetadata);
      ShardMetadata smd = new ShardMetadata();
      smd.setColumnMetadata(columnMetadata);
      store.saveShardMetadata(transaction, shardId.getTenant(), shardId.getTable(), shardId.getShardNum(), smd);
      store.commit(transaction, shardId.getTenant(), shardId.getTable(), shardId.getShardNum());
      committed = true;
      return smd;
    } finally {
      if (!committed)
        store.rollback(transaction, shardId.getTenant(), shardId.getTable(), shardId.getShardNum());
    }
  }

  public void delete(String transaction, Object entity) {
    // Remove from list
    if (captureWrite != null && captureWrite.test(shardId, ShardWriter.class.getSimpleName()))
      store.captureWrites(transaction, shardId, null, null, entity);
    for (ColumnFileWriter writer : columnFileWriters.values())
      writer.delete(transaction, entity);

  }

  public void write(String transaction, ColumnId columnId, List<WriteRequest> writeRequests) throws IOException {
    if (captureWrite != null && captureWrite.test(shardId, ShardWriter.class.getSimpleName()))
      store.captureWrites(transaction, shardId, null, writeRequests, null);

    Optional<ColumnFileWriter> opt = columnFileWriters.values().stream().filter(w -> w.getColumnId().equals(columnId)).findFirst();
    ColumnFileWriter writer;
    ColumnShardId columnShardId = null;
    if (!opt.isPresent()) {
      // The column name is not present for this shard, so lets create a new column shard by create a writer.
      columnShardId = new ColumnShardId(shardId, columnId);
      writer = store.loadColumnWriter(columnShardId);
      columnFileWriters.put(columnShardId, writer);
    } else {
      writer = opt.get();
      columnShardId = writer.getColumnShardId();
    }
    writer.write(transaction, writeRequests);
  }

  /**
   * Verifies the save request is "consistent" across columns with the shard. Part of the the consistency check
   * is to build a "entity id" column derived from the consistency check.
   */
  private ColumnMetadata consistencyCheck(String transaction, String entityIdColumn, DataType entityIdType) throws IOException {
    // First for all columns do check a do a defrag before continuing.
    for (Map.Entry<ColumnShardId, ColumnFileWriter> entry : columnFileWriters.entrySet()) {
      ColumnFileWriter cw = columnFileWriters.get(entry.getKey());
      ColumnMetadata md = cw.getMetadata();
      if (md.getFragmentationLevel() > defragTrigger.get()) {
        Instant mark = Instant.now();
        cw.defrag();
        LOGGER.info("The column fragment level for {} is at {} which is over {}, took {}",
            cw.getColumnShardId().alternateString(), md.getFragmentationLevel(), defragTrigger.get(), Duration.between(mark, Instant.now()));
      }
    }

    // Determines which should be the baseline column which is the one most entities.
    List<EntityRecordSummary> baselineSummaries = null;
    ColumnShardId baselineColumn = null;
    int maxEntities = 0;
    Map<ColumnShardId, List<EntityRecordSummary>> otherEntitiesList = new HashMap<>();
    for (Map.Entry<ColumnShardId, ColumnFileWriter> entry : columnFileWriters.entrySet()) {
      ColumnFileWriter cw = entry.getValue();
      List<EntityRecordSummary> currentSummaries = cw.getEntityRecordSummaries();
      if (currentSummaries.size() > maxEntities) {
        baselineSummaries = currentSummaries;
        maxEntities = currentSummaries.size();
        baselineColumn = cw.getColumnShardId();
      }
      otherEntitiesList.put(cw.getColumnShardId(), currentSummaries);
    }
    
    // If baseline column is null, that means the table is empty. In this case we should ensure all summaries should be set to zero.
    if (baselineColumn == null) {
      // Ensure its all zeros
      Iterator<Map.Entry<ColumnShardId, List<EntityRecordSummary>>> iterator = otherEntitiesList.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<ColumnShardId, List<EntityRecordSummary>> entry = iterator.next();
        List<EntityRecordSummary> testSummaries = entry.getValue();
        if (testSummaries.size() > 0) {
          ColumnShardId testColumn = entry.getKey();
          reportError(transaction, baselineColumn, testColumn, baselineSummaries, testSummaries);
          iterator.remove();
        }
      }
    } else {

      // Once baseline is established, find the ones that need to be defragged.
      otherEntitiesList.remove(baselineColumn);
      // For the ones that have the same number of entities, do a check to make sure
      // a) In the right order
      // b) Same number of rows
      Iterator<Map.Entry<ColumnShardId, List<EntityRecordSummary>>> iterator = otherEntitiesList.entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<ColumnShardId, List<EntityRecordSummary>> entry = iterator.next();
        List<EntityRecordSummary> testSummaries = entry.getValue();
        if (testSummaries.size() == baselineSummaries.size()) {
          ColumnShardId testColumn = entry.getKey();
          if (!testSummaries.equals(baselineSummaries)) {
            reportError(transaction, baselineColumn, testColumn, baselineSummaries, testSummaries);
          }
          iterator.remove();
        }
      }
    }

    // Remaining entries need to be resynced, meaning additional rows need to be added.
    for (ColumnShardId column : otherEntitiesList.keySet()) {
      LOGGER.info("The column {} needs to be resync according to the baseline, this may be expected if its a new column", column);
      ColumnFileWriter cw = columnFileWriters.get(column);
      cw.defrag(baselineSummaries == null ? new ArrayList<>() : baselineSummaries);
    }

    // To be extra careful, do another check with these left over columns
    for (ColumnShardId column : otherEntitiesList.keySet()) {
      ColumnFileWriter cw = columnFileWriters.get(column);
      List<EntityRecordSummary> testSummaries = cw.getEntityRecordSummaries();
      if (baselineSummaries == null) {
        if (testSummaries.size() > 0)
          reportError(transaction, baselineColumn, cw.getColumnShardId(), baselineSummaries, testSummaries);
      } else {
        if (!testSummaries.equals(baselineSummaries)) {
          reportError(transaction, baselineColumn, cw.getColumnShardId(), baselineSummaries, testSummaries);
        }
      }
    }

    ColumnId cn = new ColumnId(entityIdColumn, entityIdType.getCode());
    String randomId = UUID.randomUUID().toString();
    try (ColumnFileWriter cw = new ColumnFileWriter(new ColumnShardId(shardId, cn))) {
      List<WriteRequest> putRequests = new ArrayList<>();
      if (baselineSummaries != null) {
        for (EntityRecordSummary summary : baselineSummaries) {
          Column ecv = new Column(cn);
          int numRows = summary.getNumRows();
          Object entityId = summary.getId();
          for (int i = 0; i < numRows; i++)
            ecv.addValue(entityId);
          WriteRequest put = new WriteRequest(entityId, summary.getVersion(), randomId, ecv);
          putRequests.add(put);
        }
      }
      cw.write(transaction, putRequests);
      try {
        StreamProduct streamProduct = cw.buildInputStream(compress);
        try (InputStream inputStream = streamProduct.getInputStream()) {
          store.saveColumn(transaction, cw.getColumnShardId(), streamProduct.getByteSize(), inputStream);
        }
        return cw.getMetadata();
      } catch (Exception e) {
        LOGGER.error("Detected an issue building and saving entity column on table {} in tenant {}", shardId.getTable(), shardId.getTenant(), e);
        throw e;
      }
    }
  }

  private void reportError(
    String transaction,
    ColumnShardId baselineColumn,
    ColumnShardId testColumn,
    List<EntityRecordSummary> baselineSummaries,
    List<EntityRecordSummary> testSummaries) throws IOException {
    
    ColumnFileWriter baselineCw = null;
    if (baselineColumn != null)
      baselineCw = columnFileWriters.get(baselineColumn);
    Set<Object> baselineSummariesIds;
    if (baselineSummaries != null)
      baselineSummariesIds = baselineSummaries.stream().map(EntityRecordSummary::getId).collect(Collectors.toSet());
    else
      baselineSummariesIds = null;
    Set<Object> testSummariesIds = testSummaries.stream().map(EntityRecordSummary::getId).collect(Collectors.toSet());
    Set<Object> baselineDiff = null;
    if (baselineSummariesIds != null)
      baselineDiff = baselineSummariesIds.stream().filter(id -> !testSummariesIds.contains(id)).collect(Collectors.toSet());
    Set<Object> testDiff = testSummariesIds.stream().filter(id -> baselineSummaries != null && !baselineSummariesIds.contains(id)).collect(Collectors.toSet());
    if ((baselineDiff != null && !baselineDiff.isEmpty()) || !testDiff.isEmpty()) {
      LOGGER.error(
          "Base column {} entities don't match test column {} on table {} transaction {} base column has this {} while test column has this {}",
          baselineColumn == null ? "none" : baselineColumn.getColumnId(),
          testColumn.getColumnId(),
          baselineColumn == null ? "none" : baselineColumn.getTable(),
          transaction,
          baselineDiff,
          testDiff);
    } else {
      if (baselineSummaries != null) {
        for (int i = 0; i < baselineSummaries.size(); i++) {
          if (!baselineSummaries.get(i).equals(testSummaries.get(i))) {
            LOGGER.error("The baseline {} for {} differs from {} for {} at position (0-indexed based) {}",
                baselineColumn.toSimpleString(), baselineSummaries.get(i), testColumn.toSimpleString(), testSummaries.get(i), i);
          }
        }
      }
        
    }
    if (baselineCw != null) {
      Set<Object> corruptedBaseLine = baselineCw.getEntityDictionary().isCorrupted();
      if (!corruptedBaseLine.isEmpty()) {
        LOGGER.error("The entity dictionary in {} is corrupted check these values {}", baselineColumn, corruptedBaseLine);
      }
    }
    Set<Object> corruptedTest = columnFileWriters.get(testColumn).getEntityDictionary().isCorrupted();
    if (!corruptedTest.isEmpty()) {
      LOGGER.error("The entity dictionary in {} is corrupted check these values {}", testColumn, corruptedTest);
    }

    if (baselineCw != null) {
      StreamProduct baselineStream = baselineCw.buildInputStream(compress);
      try (InputStream is = baselineStream.getInputStream()) {
        store.saveError(transaction, baselineCw.getColumnShardId(), baselineStream.getByteSize(), is, "Entity mismatch baseline column");
      }
    }
    ColumnFileWriter testWriter = columnFileWriters.get(testColumn);
    StreamProduct testColumnStreamProduct = testWriter.buildInputStream(compress);
    try (InputStream is = testColumnStreamProduct.getInputStream()) {
      store.saveError(transaction, testWriter.getColumnShardId(), testColumnStreamProduct.getByteSize(), is, "Entity mismatch test column");
    }

    // Save other columns for analysis
    for (Map.Entry<ColumnShardId, ColumnFileWriter> e1 : columnFileWriters.entrySet()) {
      ColumnFileWriter cw = e1.getValue();
      if (cw.getColumnId().equals(baselineColumn == null ? "none" : baselineColumn.getColumnId()) || cw.getColumnId().equals(testColumn.getColumnId())) {
        continue;
      }
      StreamProduct otherColumnStreamProduct = cw.buildInputStream(compress);
      try (InputStream is = otherColumnStreamProduct.getInputStream()) {
        store.saveError(transaction, cw.getColumnShardId(), otherColumnStreamProduct.getByteSize(), is, null);
      }
    }

    throw new RuntimeException("The entity summaries do not match the baseline entity summaries on " + shardId.getTable() + " " + shardId);
  }
}
