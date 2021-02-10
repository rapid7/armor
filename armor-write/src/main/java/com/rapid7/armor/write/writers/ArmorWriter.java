package com.rapid7.armor.write.writers;

import com.rapid7.armor.entity.Column;
import com.rapid7.armor.entity.Entity;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.meta.TableMetadata;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.WriteStore;
import com.rapid7.armor.store.WriteTranscationError;
import com.rapid7.armor.write.EntityOffsetException;
import com.rapid7.armor.write.TableId;
import com.rapid7.armor.write.WriteRequest;

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
  private final Map<TableId, TableWriter> tableWriters = new ConcurrentHashMap<>();
  private final Map<TableId, ColumnId> tableEntityColumnIds = new ConcurrentHashMap<>();

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

  public Map<Integer, EntityRecord> getColumnEntityRecords(String tenant, String table, String columnId, int shard) {
    TableId tableId = new TableId(tenant, table);
    TableWriter tableWriter = tableWriters.get(tableId);
    if (tableWriter == null) {
      // There is no writer, so we should first check to see if a metadata file exists for this table.
      TableMetadata tableMeta = store.loadTableMetadata(tenant, table);
      if (tableMeta != null) {
        // Metadata file exists, meaning the table exists, use the metadata to determine.
        tableWriter = new TableWriter(tenant, table, store);
        tableWriters.put(tableId, tableWriter);
        tableEntityColumnIds.put(tableId, toColumnId(tableMeta));
        ShardId shardId = store.buildShardId(tenant, table, shard);
        ShardWriter sw = new ShardWriter(shardId, store, compress, defragTrigger, captureWrites);
        sw = tableWriter.addShard(sw);
        return sw.getEntities(columnId);
      } else {
        return null;
      }
    } else {
      // Tablewriter exists so just return what we need.
      ShardWriter sw = tableWriter.getShard(shard);
      if (sw == null) {
        ShardId shardId = store.buildShardId(tenant, table, shard);
        sw = new ShardWriter(shardId, store, compress, defragTrigger, captureWrites);
        sw = tableWriter.addShard(sw);
      }
      return sw.getEntities(columnId);
    }
  }

  public ColumnMetadata getColumnMetadata(String tenant, String table, String columnId, int shard) {
    TableId tableId = new TableId(tenant, table);
    TableWriter tableWriter = tableWriters.get(tableId);
    if (tableWriter == null) {
      TableMetadata tableMeta = store.loadTableMetadata(tenant, table);
      if (tableMeta != null) {
        // The table does exist, load it up then
        tableWriter = new TableWriter(tenant, table, store);
        tableWriters.put(tableId, tableWriter);
        tableEntityColumnIds.put(tableId, toColumnId(tableMeta));

        ShardId shardId = store.buildShardId(tenant, table, shard);
        ShardWriter sw = new ShardWriter(shardId, store, compress, defragTrigger, captureWrites);
        sw = tableWriter.addShard(sw);
        return sw.getMetadata(columnId);
      } else
        return null;
    } else {
      ShardWriter sw = tableWriter.getShard(shard);
      if (sw == null) {
        ShardId shardId = store.buildShardId(tenant, table, shard);
        sw = new ShardWriter(shardId, store, compress, defragTrigger, captureWrites);
        sw = tableWriter.addShard(sw);
      }
      return sw.getMetadata(columnId);
    }
  }

  public void close() {
    if (selfPool)
      threadPool.shutdown();
    for (TableWriter table : tableWriters.values()) {
      try {
        table.close();
      } catch (IOException ioe) {
        LOGGER.warn("Unable to close table {}", table.getTableName(), ioe);
      }
    }
  }

  public void delete(String transaction, String tenant, String table, Object entityId) {
    ShardId shardId = store.findShardId(tenant, table, entityId);
    if (captureWrites != null && captureWrites.test(shardId, ArmorWriter.class.getSimpleName()))
      store.captureWrites(transaction, shardId, null, null, entityId);
    TableId tableId = new TableId(tenant, table);
    TableWriter tableWriter = tableWriters.get(tableId);
    if (tableWriter != null) {
      // This occurs if a write happened first then delete.
      ShardWriter sw = tableWriter.getShard(shardId.getShardNum());
      if (sw != null) {
        sw.delete(transaction, entityId);
        return;
      } else {
        // NOTE: Let it fall through, since its a new shard we haven't loaded yet.
      }
      
    }

    // If it is null then table doesn't exist yet which means we can just return.
    // If it is not null then table does exist, in that case load it up and attempt a delete.
    TableMetadata tableMeta = store.loadTableMetadata(tenant, table);
    if (tableMeta == null)
      return;

    if (tableWriter == null) {
      tableWriter = new TableWriter(
        tenant,
        table,
        store);
    }
    tableWriters.put(tableId, tableWriter);
    tableEntityColumnIds.put(tableId, toColumnId(tableMeta));

    ShardWriter sw = tableWriter.getShard(shardId.getShardNum());
    if (sw == null) {
      sw = new ShardWriter(shardId, store, compress, defragTrigger, captureWrites);
      sw = tableWriter.addShard(sw);
    }
    sw.delete(transaction, entityId);
  }
  
  /**
   * Builds an initial tablemetadata that defines the table's intended id and type from an Entity object.
   */
  private ColumnId buildEntityColumnId(Entity entity) {
    Object entityId = entity.getEntityId();
    DataType entityIdType = DataType.INTEGER;
    if (entityId instanceof String)
      entityIdType = DataType.STRING;
    String entityIdColumn = entity.getEntityIdColumn();
    return new ColumnId(entityIdColumn, entityIdType.getCode());
  }
  
  private ColumnId toColumnId(TableMetadata tableMetadata) {
    if (tableMetadata == null)
      return null;
    return new ColumnId(tableMetadata.getEntityColumnId(), tableMetadata.getEntityColumnIdType());
  }

  public void write(String transaction, String tenant, String table, List<Entity> entities) {
    if (entities == null || entities.isEmpty())
      return;
    if (captureWrites != null && captureWrites.test(new ShardId(-1, tenant, table), ArmorWriter.class.getSimpleName()))
      store.captureWrites(transaction, new ShardId(-1, tenant, table), entities, null, null);


    HashMap<ShardId, List<Entity>> shardToUpdates = new HashMap<>();
    TableId tableId = new TableId(tenant, table);
    final TableWriter tableWriter;
    if (!tableWriters.containsKey(tableId)) {
      TableMetadata tableMeta = store.loadTableMetadata(tenant, table);
      if (tableMeta != null) {
        // The table exists, load it up then
        tableWriter = new TableWriter(tenant, table, store);
        tableWriters.put(tableId, tableWriter);
        tableEntityColumnIds.put(tableId, toColumnId(tableMeta));
      } else {
        Entity entity = entities.get(0);
        // No shard metadata exists, create the first shard metadata for this.
        tableWriter = new TableWriter(tenant, table, store);
        tableWriters.put(tableId, tableWriter);
        tableEntityColumnIds.put(tableId, buildEntityColumnId(entity));
      }
    } else {
      tableWriter = tableWriters.get(tableId);
    }
    for (Entity entity : entities) {
      ShardId shardId = store.findShardId(tenant, table, entity.getEntityId());
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
              MDC.put("tenant_id", tenant);
              ShardId shardId = entry.getKey();
              ShardWriter shardWriter = tableWriter.getShard(shardId.getShardNum());
              Thread.currentThread().setName(originalThreadName + "(" + shardId.toString() + ")");
              if (shardWriter == null) {
                shardWriter = new ShardWriter(shardId, store, compress, defragTrigger, captureWrites);
                shardWriter = tableWriter.addShard(shardWriter);
              }

              List<Entity> entityUpdates = entry.getValue();
              Map<ColumnId, List<WriteRequest>> columnIdEntityColumns = new HashMap<>();
              for (Entity eu : entityUpdates) {
                Object entityId = eu.getEntityId();
                long version = eu.getVersion();
                String instanceid = eu.getInstanceId();
                for (Column ec : eu.columns()) {
                  WriteRequest internalRequest = new WriteRequest(entityId, version, instanceid, ec);
                  List<WriteRequest> payloads = columnIdEntityColumns.computeIfAbsent(
                      ec.getColumnId(),
                      k -> new ArrayList<>()
                  );
                  payloads.add(internalRequest);
                }
              }
              for (Map.Entry<ColumnId, List<WriteRequest>> e : columnIdEntityColumns.entrySet()) {
                ColumnId columnId = e.getKey();
                List<WriteRequest> columns = e.getValue();
                shardWriter.write(transaction, columnId, columns);
              }
              return null;
            } finally {
              MDC.remove("tenant_id");
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

  public void commit(String transaction, String tenant, String table) {
    CompletionService<ShardMetadata> std = new ExecutorCompletionService<>(threadPool);
    int submitted = 0;
    TableId tableId = new TableId(tenant, table);
    TableWriter tableWriter = tableWriters.get(tableId);
    if (tableWriter == null) {
      LOGGER.warn("There are no changes detected, going to skip save operation for {}", tableId);
      return;
    }
    ColumnId entityColumnId = tableEntityColumnIds.get(tableId);
    if (entityColumnId == null) {
      TableMetadata tableMetadata = this.store.loadTableMetadata(tenant, table);
      if (tableMetadata == null) {
        throw new RuntimeException("Unable to determine the entityid column name from store or memory, cannot commit");
      }
      entityColumnId = toColumnId(tableMetadata);
    }
    final ColumnId finalEntityColumnId = entityColumnId;
    List<EntityOffsetException> offsetExceptions = new ArrayList<>();
    for (ShardWriter shardWriter : tableWriter.getShardWriters()) {
      std.submit(
          () -> {
            String originalName = Thread.currentThread().getName();
            try {
              Thread.currentThread().setName("shardwriter-" + shardWriter.getShardId());
              MDC.put("tenant_id", tenant);
              return shardWriter.commit(transaction, finalEntityColumnId);
            } catch (NoSuchFileException nse) {
              LOGGER.warn("The underlying channels file are missing, most likely closed by another fried due to an issue: {}", nse.getMessage());
              return null;
            } catch (ClosedChannelException cce) {
              LOGGER.warn("The underlying channels are closed in {}, most likely closed by another thread due to an issue: {}", shardWriter.getShardId(), cce.getMessage());
              return null;
            } catch (EntityOffsetException e1) {
              offsetExceptions.add(e1);
              throw e1;
            } catch (Exception e) {
              LOGGER.error("Detected an error on shard {} table {} in tenant {}", 
                shardWriter.getShardId(), tableWriter.getTableName(), tableWriter.getTenant(), e);
              throw e;
            } finally {
              Thread.currentThread().setName(originalName);
              MDC.remove("tenant_id");
            }
        }
      );
      submitted++;
    }
    List<ShardMetadata> shardMetaDatas = new ArrayList<>();
    TableMetadata tableMetadata = new TableMetadata(entityColumnId.getName(), entityColumnId.getType());
    tableMetadata.setShardMetadata(shardMetaDatas);
    for (int i = 0; i < submitted; ++i) {
      try {
        shardMetaDatas.add(std.take().get());
      } catch (InterruptedException | ExecutionException e) {
        // Throw specialized handlers up verses wrapped runtime exceptions
        if (!offsetExceptions.isEmpty()) {
          EntityOffsetException offsetException = offsetExceptions.get(0);
          LOGGER.error("!!!Detected an error on table {} in tenant {}", tableWriter.getTableName(), tableWriter.getTenant(), e);
          LOGGER.error(offsetException.getMessage());
          throw offsetException;
        }
        if (e.getCause() instanceof WriteTranscationError) {
          throw (WriteTranscationError) e.getCause();
        }
        throw new RuntimeException(e);
      }
    }
    store.saveTableMetadata(transaction, tenant, table, tableMetadata);
  }
}
