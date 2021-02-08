package com.rapid7.armor.write;

import com.rapid7.armor.entity.Column;
import com.rapid7.armor.entity.Entity;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.meta.TableMetadata;
import com.rapid7.armor.schema.ColumnName;
import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.WriteStore;
import com.rapid7.armor.store.WriteTranscationError;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public class ArmorWriter implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArmorWriter.class);
  private final ExecutorService threadPool;
  private final WriteStore store;
  private final Map<TableWriteId, TableWrite> tables = new ConcurrentHashMap<>();
  private final Supplier<Integer> defragTrigger;
  private boolean selfPool = true;
  private final BiPredicate<ShardId, String> captureWrites;
  private boolean compress = false;
  private String name;

  public ArmorWriter(String name, WriteStore store, boolean compress, int numThreads) {
    this.store = store;
    this.threadPool = Executors.newFixedThreadPool(numThreads);
    this.selfPool = true;
    this.compress = compress;
    this.name = name;
    this.defragTrigger = () -> 50;
    this.captureWrites = null;
  }

  public ArmorWriter(String name, WriteStore store, boolean compress, int numThreads, Supplier<Integer> defragTrigger, BiPredicate<ShardId, String> captureWrites) {
    this.store = store;
    this.threadPool = Executors.newFixedThreadPool(numThreads);
    this.selfPool = true;
    this.captureWrites = captureWrites;
    this.compress = compress;
    this.name = name;
    if (defragTrigger == null) {
      this.defragTrigger = () -> 50;
    } else
      this.defragTrigger = defragTrigger;
  }

  public ArmorWriter(String name, WriteStore store, boolean compress, ExecutorService pool, Supplier<Integer> defragTrigger, BiPredicate<ShardId, String> captureWrites) {
    this.store = store;
    this.threadPool = pool;
    this.captureWrites = captureWrites;
    this.selfPool = false;
    this.name = name;
    this.compress = compress;
    if (defragTrigger == null) {
      this.defragTrigger = () -> 50;
    } else
      this.defragTrigger = defragTrigger;
  }

  public String getName() {
    return name;
  }

  public String startTransaction() {
    return UUID.randomUUID().toString();
  }

  public Map<Integer, EntityRecord> getColumnEntityRecords(String org, String table, String columnName, int shard) {
    TableWriteId tableId = new TableWriteId(org, table);
    TableWrite tableDefinition = tables.get(tableId);
    if (tableDefinition == null) {
      // The table does exist, load it up then
      TableMetadata tableMeta = store.loadTableMetadata(org, table);
      if (tableMeta != null) {
        tableDefinition = new TableWrite(org, table, tableMeta.getEntityColumnId(), tableMeta.dataType(), store);
        tables.put(tableId, tableDefinition);
        ShardId shardId = store.buildShardId(org, table, shard);
        ShardWriter sw = new ShardWriter(tableDefinition, shardId, store, compress, defragTrigger, captureWrites);
        tableDefinition.addShard(sw);
        return sw.getEntities(columnName);
      } else {
        return null;
      }
    } else {
      ShardWriter sw = tableDefinition.getShard(shard);
      if (sw == null) {
        ShardId shardId = store.buildShardId(org, table, shard);
        sw = new ShardWriter(tableDefinition, shardId, store, compress, defragTrigger, captureWrites);
        tableDefinition.addShard(sw);
      }
      return sw.getEntities(columnName);
    }
  }

  public ColumnMetadata getColumnMetadata(String org, String table, String columnName, int shard) {
    TableWriteId tableId = new TableWriteId(org, table);
    TableWrite tableDefinition = tables.get(tableId);
    if (tableDefinition == null) {
      TableMetadata tableMeta = store.loadTableMetadata(org, table);
      if (tableMeta != null) {
        // The table does exist, load it up then
        tableDefinition = new TableWrite(org, table, tableMeta.getEntityColumnId(), tableMeta.dataType(), store);
        tables.put(tableId, tableDefinition);

        ShardId shardId = store.buildShardId(org, table, shard);
        ShardWriter sw = new ShardWriter(tableDefinition, shardId, store, compress, defragTrigger, captureWrites);
        tableDefinition.addShard(sw);
        return sw.getMetadata(columnName);
      } else
        return null;
    } else {
      ShardWriter sw = tableDefinition.getShard(shard);
      if (sw == null) {
        ShardId shardId = store.buildShardId(org, table, shard);
        sw = new ShardWriter(tableDefinition, shardId, store, compress, defragTrigger, captureWrites);
        tableDefinition.addShard(sw);
      }
      return sw.getMetadata(columnName);
    }
  }

  public void close() {
    if (selfPool)
      threadPool.shutdown();
    for (TableWrite table : tables.values()) {
      try {
        table.close();
      } catch (IOException ioe) {
        LOGGER.warn("Unable to close table {}", table.getTableName(), ioe);
      }
    }
  }

  public void delete(String transaction, String org, String table, Object entityId) {
    ShardId shardId = store.findShardId(org, table, entityId);
    if (shardId.getShardNum() == 8)
      System.out.println("");
    if (captureWrites != null && captureWrites.test(shardId, ArmorWriter.class.getSimpleName()))
      store.captureWrites(transaction, shardId, null, null, entityId);
    TableWriteId tableId = new TableWriteId(org, table);
    TableWrite tableWriter = tables.get(tableId);
    if (tableWriter != null) {
      ShardWriter sw = tableWriter.getShard(shardId.getShardNum());
      if (sw != null) {
        sw.delete(transaction, entityId);
        return;
      }
    }
    // If it is null then table doesn't exist yet which means we can just return.
    // If it is not null then table does exist, in that case load it up and attempt a delete.
    TableMetadata tableMeta = store.loadTableMetadata(org, table);
    if (tableMeta == null)
      return;
    if (tableWriter == null) {
      tableWriter = new TableWrite(
        org,
        table,
        tableMeta.getEntityColumnId(),
        DataType.getDataType(tableMeta.getEntityColumnIdType()),
        store);
    }
    tables.put(tableId, tableWriter);
    ShardWriter sw = tableWriter.getShard(shardId.getShardNum());
    if (sw == null) {
      sw = new ShardWriter(tableWriter, shardId, store, compress, defragTrigger, captureWrites);
      tableWriter.addShard(sw);
    }
    System.out.println("Delete on shard " + shardId.getShardNum());
    sw.delete(transaction, entityId);
  }

  private void createTableMetadata(String transaction, TableWrite tableWrite) {
    store.saveTableMetadata(transaction, tableWrite.getOrg(), tableWrite.getTableName(), tableWrite.toTableMetadata());
  }

  public void write(String transaction, String org, String table, List<Entity> entities) {
    if (entities == null || entities.isEmpty())
      return;
    if (captureWrites != null && captureWrites.test(new ShardId(-1, org, table), ArmorWriter.class.getSimpleName()))
      store.captureWrites(transaction, new ShardId(-1, org, table), entities, null, null);


    HashMap<ShardId, List<Entity>> shardToUpdates = new HashMap<>();
    TableWriteId tableId = new TableWriteId(org, table);
    final TableWrite tableWrite;
    if (!tables.containsKey(tableId)) {
      TableMetadata tableMeta = store.loadTableMetadata(org, table);
      if (tableMeta != null) {
        // The table does exist, load it up then
        tableWrite = new TableWrite(org, table, tableMeta.getEntityColumnId(), tableMeta.dataType(), store);
        tables.put(tableId, tableWrite);
      } else {
        Entity entity = entities.get(0);
        Object entityId = entity.getEntityId();
        DataType dataType = DataType.INTEGER;
        if (entityId instanceof String)
          dataType = DataType.STRING;
        String entityColumn = entity.getEntityIdColumn();
        // No shard metadata exists, create the first shard metadata for this.
        tableWrite = new TableWrite(org, table, entityColumn, dataType, store);
        tables.put(tableId, tableWrite);
        createTableMetadata(transaction, tableWrite);
      }
    } else {
      tableWrite = tables.get(tableId);
    }
    for (Entity entity : entities) {
      ShardId shardId = store.findShardId(org, table, entity.getEntityId());
      List<Entity> entityUpdates = shardToUpdates.computeIfAbsent(shardId, k -> new ArrayList<>());
      entityUpdates.add(entity);
    }
    int numShards = shardToUpdates.size();
    ExecutorCompletionService<Void> ecs = new ExecutorCompletionService<>(threadPool);
    for (Map.Entry<ShardId, List<Entity>> entry : shardToUpdates.entrySet()) {
      ecs.submit(
          () -> {
            String originalThreadName = Thread.currentThread().getName();
            try {
              MDC.put("org_id", org);
              ShardId shardId = entry.getKey();
              ShardWriter shardWriter = tableWrite.getShard(shardId.getShardNum());
              Thread.currentThread().setName(originalThreadName + "(" + shardId.toString() + ")");
              if (shardWriter == null) {
                shardWriter = new ShardWriter(tableWrite, shardId, store, compress, defragTrigger, captureWrites);
                tableWrite.addShard(shardWriter);
              }

              List<Entity> entityUpdates = entry.getValue();
              Map<ColumnName, List<WriteRequest>> columnNameEntityColumns = new HashMap<>();
              for (Entity eu : entityUpdates) {
                Object entityId = eu.getEntityId();
                long version = eu.getVersion();
                String instanceid = eu.getInstanceId();
                for (Column ec : eu.columns()) {
                  WriteRequest internalRequest = new WriteRequest(entityId, version, instanceid, ec);
                  List<WriteRequest> payloads = columnNameEntityColumns.computeIfAbsent(
                      ec.getColumnName(),
                      k -> new ArrayList<>()
                  );
                  payloads.add(internalRequest);
                }
              }
              for (Map.Entry<ColumnName, List<WriteRequest>> e : columnNameEntityColumns.entrySet()) {
                ColumnName columnName = e.getKey();
                List<WriteRequest> columns = e.getValue();
                shardWriter.write(transaction, columnName, columns);
              }
              return null;
            } finally {
              MDC.remove("org_id");
              Thread.currentThread().setName(originalThreadName);
            }
        }
      );
    }
    for (int i = 0; i < numShards; ++i) {
      try {
        ecs.take().get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public void commit(String transaction, String org, String table) {
    CompletionService<ShardMetadata> std = new ExecutorCompletionService<>(threadPool);
    int submitted = 0;
    TableWriteId tableId = new TableWriteId(org, table);
    TableWrite tableDefinition = tables.get(tableId);
    if (tableDefinition == null) {
      LOGGER.warn("There are no changes detected, going to skip save operation for {}", tableId);
      return;
    }
    List<EntityOffsetException> offsetExceptions = new ArrayList<>();
    for (ShardWriter shardWriter : tableDefinition.getShardWriters()) {
      std.submit(
          () -> {
            String originalName = Thread.currentThread().getName();
            try {
              Thread.currentThread().setName("shardwriter-" + shardWriter.getShardId());
              MDC.put("org_id", org);
              return shardWriter.save(transaction);
            } catch (NoSuchFileException nse) {
              LOGGER.warn("The underlying channels file are missing, most likely closed by another fried due to an issue: {}", nse.getMessage());
              return null;
            } catch (ClosedChannelException cce) {
              LOGGER.warn("The underlying channels are closed, most likely closed by another thread due to an issue: {}", cce.getMessage());
              return null;
            } catch (EntityOffsetException e1) {
              offsetExceptions.add(e1);
              throw e1;
            } catch (Exception e) {
              LOGGER.error("Detected an error on shard {} table {} in tenant {}", 
                shardWriter.getShardId(), tableDefinition.getTableName(), tableDefinition.getOrg(), e);
              throw e;
            } finally {
              Thread.currentThread().setName(originalName);
              MDC.remove("org_id");
            }
        }
      );
      submitted++;
    }
    TableMetadata tmd = tableDefinition.toTableMetadata();
    List<ShardMetadata> shardMetaDatas = new ArrayList<>();
    tmd.setShardMetadata(shardMetaDatas);
    for (int i = 0; i < submitted; ++i) {
      try {
        shardMetaDatas.add(std.take().get());
      } catch (InterruptedException | ExecutionException e) {
        // Throw specialized handlers up verses wrapped runtime exceptions
        if (!offsetExceptions.isEmpty()) {
          EntityOffsetException offsetException = offsetExceptions.get(0);
          LOGGER.error("!!!Detected an error on table {} in org {}", tableDefinition.getTableName(), tableDefinition.getOrg(), e);
          LOGGER.error(offsetException.getMessage());
          throw offsetException;
        }
        if (e.getCause() instanceof WriteTranscationError) {
          throw (WriteTranscationError) e.getCause();
        }
        throw new RuntimeException(e);
      }
    }
    store.saveTableMetadata(transaction, org, table, tmd);
  }
}
