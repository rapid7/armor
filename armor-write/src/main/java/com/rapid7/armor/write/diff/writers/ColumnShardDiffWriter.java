package com.rapid7.armor.write.diff.writers;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rapid7.armor.entity.Column;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.entity.EntityRecordSummary;
import com.rapid7.armor.io.Compression;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.WriteStore;
import com.rapid7.armor.write.StreamProduct;
import com.rapid7.armor.write.WriteRequest;
import com.rapid7.armor.write.component.RowGroupWriter;
import com.rapid7.armor.write.writers.ColumnFileWriter;
import com.rapid7.armor.write.writers.IShardWriter;

/**
 * Handles writes for one or more columns for a shard in one "atomic" operation.
 */
public class ColumnShardDiffWriter implements IShardWriter {
  private static final Logger LOGGER = LoggerFactory.getLogger(ColumnShardDiffWriter.class);
  private final WriteStore store;
  private ColumnId columnId;

  private ColumnFileWriter baselineColumnFile;
  private boolean diffForNew = false;
  
  private final ShardId targetShardId;
  private final ColumnShardId targetColumnShardId;
  private ColumnFileWriter targetColumnFile;

  private final Supplier<Integer> compactionTrigger;
  private Compression compress = Compression.ZSTD;
  
  public ShardId getTargetShardId() {
    return targetShardId;
  }

  public ColumnShardDiffWriter(
    ShardId targetShardId,
    ColumnFileWriter baselineColumnFile,
    boolean diffForNew,
    ColumnId columnId,
    WriteStore store,
    Compression compress,
    Supplier<Integer> compactionTriggerSupplier) {
    this.targetShardId = targetShardId;
    this.store = store;
    this.compress = compress;
    if (compactionTriggerSupplier == null)
      this.compactionTrigger = () -> 90;
    else
      this.compactionTrigger = compactionTriggerSupplier;
    this.diffForNew = diffForNew;
    targetColumnShardId = new ColumnShardId(targetShardId, columnId);
    
    targetColumnFile = store.loadColumnWriter(targetColumnShardId);
    this.baselineColumnFile = baselineColumnFile;
  }

  public void close() {
    try {
      targetColumnFile.close();
    } catch (Exception e) {
      LOGGER.warn("Unable to close column {}", columnId, e);
    }
    if (baselineColumnFile != null) {
      try {
        baselineColumnFile.close();
      } catch (Exception e) {
        LOGGER.warn("Unable to close column {}", columnId, e);
      }
    }
  }

  /**
   * Commits the any changes the shard writer has been writing too.
   *
   * @param transaction The transaction of the writes.
   * @param columnEntityId The id for the entity column for this table.
   *
   * @return The metadata of the shard just written.
   * 
   * @throws IOException If an io error occurs.
   */
  public ShardMetadata commit(String transaction, ColumnId columnEntityId) throws IOException {
    boolean committed = false;
    try {
      ColumnMetadata entityColumnMetadata = consistencyCheck(transaction, columnEntityId.getName(), columnEntityId.dataType());

      StreamProduct streamProduct = targetColumnFile.buildInputStream(compress);
      try (InputStream inputStream = streamProduct.getInputStream()) {
        store.saveColumn(transaction, targetColumnShardId, streamProduct.getByteSize(), inputStream);
      }
      
      // Do this after the save, to ensure metadata is updated.
      List<ColumnMetadata> columnMetadata = new ArrayList<>();
      columnMetadata.add(targetColumnFile.getMetadata());
      columnMetadata.add(entityColumnMetadata);
      ShardMetadata smd = new ShardMetadata(targetShardId, columnMetadata);
      store.saveShardMetadata(transaction, smd);
      store.commit(transaction, targetShardId);
      committed = true;
      return smd;
    } finally {
      if (!committed)
        store.rollback(transaction, targetShardId);
    }
  }

  public void delete(String transaction, Object entity, long version, String instanceId) {
    targetColumnFile.delete(transaction, entity, version, instanceId);
  }

  public void writeDiff(String transaction, List<WriteRequest> writeRequests) throws IOException {
    if (baselineColumnFile == null) {
      if (diffForNew) {
        for (WriteRequest wr : writeRequests) {
          Set<Object> newValues = new HashSet<>(wr.getColumn().getValues());
          wr.getColumn().setValues(new ArrayList<>(newValues));
        }
        targetColumnFile.write(transaction, writeRequests);
      }
    } else {
      RowGroupWriter rgw = baselineColumnFile.getRowGroupWriter();
      List<WriteRequest> toWrite = new ArrayList<>();
      for (WriteRequest wr : writeRequests) {
        Integer entityId = baselineColumnFile.getEntityId(wr.getEntityId());
        EntityRecord er = entityId != null ? baselineColumnFile.getEntites().get(entityId) : null;
        if (er != null) {
          if (diffForNew) {
            Set<Object> baseLineValues = new HashSet<>(rgw.getEntityValues(er));
            Set<Object> newTargetValues2 = new HashSet<>(wr.getColumn().getValues());
            newTargetValues2.removeAll(baseLineValues);
            if (!newTargetValues2.isEmpty()) {
              wr.getColumn().setValues(new ArrayList<>(newTargetValues2));
              toWrite.add(wr);
            } else {
              // Its empty meaning no changes, so we should check to see if it already exists in the targeted baseline.
              // If so then we should DELETE so that there is no entry.
              EntityRecord targetEr = targetColumnFile.getEntites().get(entityId);
              if (targetEr != null && targetEr.getDeleted() == 0) {
                targetColumnFile.delete(transaction, wr.getEntityId(), wr.getVersion(), wr.getInstanceId());
              }
            }
              
          } else {
            Set<Object> removedBaselineValues = new HashSet<>(rgw.getEntityValues(er));
            Set<Object> targetValues = new HashSet<>(wr.getColumn().getValues());
            removedBaselineValues.removeAll(targetValues);
            if (!removedBaselineValues.isEmpty()) {
              wr.getColumn().setValues(new ArrayList<>(removedBaselineValues));
              toWrite.add(wr);
            } else {
              // Its empty meaning no changes, so we should check to see if it already exists in the targeted baseline.
              // If so then we should DELETE so that there is no entry.
              EntityRecord targetEr = targetColumnFile.getEntites().get(entityId);
              if (targetEr != null && targetEr.getDeleted() == 0) {
                targetColumnFile.delete(transaction, wr.getEntityId(), wr.getVersion(), wr.getInstanceId());
              }
            }
          }          
        } else if (diffForNew) {
          Set<Object> newValues = new HashSet<>(wr.getColumn().getValues());
          wr.getColumn().setValues(new ArrayList<>(newValues));
          toWrite.add(wr);
        }
      }
      targetColumnFile.write(transaction, toWrite);
    }
  }

  /**
   * Verifies the save request is "consistent" across columns with the shard. Part of the the consistency check
   * is to build a "entity id" column derived from the consistency check.
   */
  private ColumnMetadata consistencyCheck(String transaction, String entityIdColumn, DataType entityIdType) throws IOException {
    // First for all columns do check a do a compaction before continuing.
   ColumnFileWriter cw = targetColumnFile;
   ColumnMetadata md = cw.getMetadata();
   if (md.getFragmentationLevel() > compactionTrigger.get()) {
     Instant mark = Instant.now();
     cw.compact();
     LOGGER.info("The column fragment level for {} is at {} which is over {}, took {}",
         cw.getColumnShardId().alternateString(), md.getFragmentationLevel(), compactionTrigger.get(), Duration.between(mark, Instant.now()));
   }
    
    // Determines which should be the baseline column which is the one most entities.
    List<EntityRecordSummary> baselineSummaries = targetColumnFile.getEntityRecordSummaries();
    if (baselineSummaries.isEmpty())
      return null;
    return buildStoreEntityIdColumn(transaction, baselineSummaries, entityIdColumn, entityIdType);
  }
  
  /**
   * Builds and store the entity id column. This entity id column is based off a given baseline. The baseline should be checked for consistency before
   * it passed into this method.
   */
  private ColumnMetadata buildStoreEntityIdColumn(String transaction, List<EntityRecordSummary> baselineSummaries, String entityIdColumn, DataType entityIdType)
    throws IOException {
    ColumnId cn = new ColumnId(entityIdColumn, entityIdType.getCode());
    String randomId = UUID.randomUUID().toString();
    try (ColumnFileWriter cw = new ColumnFileWriter(new ColumnShardId(targetShardId, cn))) {
      cw.getMetadata().setEntityId(true);
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
        LOGGER.error("Detected an issue building and saving entity column on table {} in tenant {}", targetShardId.getTable(), targetShardId.getTenant(), e);
        throw e;
      }
    }
  }

  @Override
  public ShardId getShardId() {
    return targetShardId;
  }

  @Override
  public ColumnMetadata getMetadata(String columnId) {
    return targetColumnFile.getMetadata();
  }

  @Override
  public void write(String transaction, ColumnId columnId, List<WriteRequest> columns) throws IOException {
    writeDiff(transaction, columns);
  }

  @Override
  public Map<Integer, EntityRecord> getEntities(String columnId) {
    return targetColumnFile.getEntites();
  }
}
