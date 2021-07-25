package com.rapid7.armor.write.writers;

import java.io.Closeable;
import java.nio.channels.ClosedChannelException;
import java.nio.file.NoSuchFileException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiPredicate;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import com.rapid7.armor.entity.Column;
import com.rapid7.armor.entity.Entity;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.io.Compression;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.WriteStore;
import com.rapid7.armor.write.EntityOffsetException;
import com.rapid7.armor.write.TableId;
import com.rapid7.armor.write.WriteRequest;
import com.rapid7.armor.xact.XactError;

public class ArmorWriter implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ArmorWriter.class);
  private final ExecutorService threadPool;
  private final WriteStore store;
  private final Map<TableId, TableWriter> tableWriters = new ConcurrentHashMap<>();
  private final Map<TableId, ColumnId> tableEntityColumnIds = new ConcurrentHashMap<>();

  private final Supplier<Integer> compactionTrigger;
  private boolean selfPool = true;
  private final BiPredicate<ShardId, String> captureWrites;
  private Compression compress = Compression.ZSTD;
  private String name;
  private String transaction;
 
  public ArmorWriter(String name, WriteStore store, Compression compress, int numThreads) {
    this.store = store;
    this.threadPool = Executors.newFixedThreadPool(numThreads);
    this.selfPool = true;
    this.compress = compress;
    this.name = name;
    this.compactionTrigger = () -> 50;
    this.captureWrites = null;
  }

  public ArmorWriter(String name, WriteStore store, Compression compress, int numThreads, Supplier<Integer> compactionTrigger, BiPredicate<ShardId, String> captureWrites) {
    this.store = store;
    this.threadPool = Executors.newFixedThreadPool(numThreads);
    this.selfPool = true;
    this.captureWrites = captureWrites;
    this.compress = compress;
    this.name = name;
    if (compactionTrigger == null) {
      this.compactionTrigger = () -> 20;
    } else
      this.compactionTrigger = compactionTrigger;
  }

  /**
   * Constructs an ArmorWriter.
   *
   * @param name Assign a name to a writer.
   * @param store A write store instance it should be running against.
   * @param compress The compression option.
   * @param pool A thread pool to use where one thread is run per shard for writing.
   * @param compactionTrigger Supply a setting of when to start compaction.
   * @param captureWrites Predicate to determine when to trigger capturing write requests.
   */
  public ArmorWriter(String name, WriteStore store, Compression compress, ExecutorService pool, Supplier<Integer> compactionTrigger, BiPredicate<ShardId, String> captureWrites) {
    this.store = store;
    this.threadPool = pool;
    this.captureWrites = captureWrites;
    this.selfPool = false;
    this.name = name;
    this.compress = compress;
    if (compactionTrigger == null) {
      this.compactionTrigger = () -> 50;
    } else
      this.compactionTrigger = compactionTrigger;
  }

  public String getName() {
    return name;
  }

  public void begin() {
    transaction = UUID.randomUUID().toString();
    for (TableWriter tw : tableWriters.values()) {
      for (IShardWriter sw : tw.getShardWriters()) {
        sw.begin(transaction);
      }
    }
  }

  public Map<Integer, EntityRecord> columnEntityRecords(String tenant, String table, Interval interval, Instant timestamp, String columnId, int shard) {
    TableId tableId = new TableId(tenant, table);
    TableWriter tableWriter = tableWriters.get(tableId);
    if (tableWriter == null) {
        return null;
    } else {
      IShardWriter sw = tableWriter.getShard(new ShardId(tenant, table, interval.getInterval(), interval.getIntervalStart(timestamp), shard));
      if (sw == null)
        return null;
      else 
        return sw.getEntities(columnId);
    }
  }

  /**
   * Returns the columnMetadata IF it was already loaded. If it hasn't been loaded by the writer, it will
   * simply return null;
   * 
   * @param tenant The tenant.
   * @param table The table in question.
   * @param interval A span of time in minutes by which to group data
   * @param timestamp The point in time in which to bucket the data as per the interval
   * @param columnId The id of the column.
   * @param shard The shard number.
   * 
   * @return The column metadata.
   */
  public ColumnMetadata columnMetadata(String tenant, String table, Interval interval, Instant timestamp, String columnId, int shard) {
    TableId tableId = new TableId(tenant, table);
    TableWriter tableWriter = tableWriters.get(tableId);
    if (tableWriter == null) {
      return null;
    } else {
      IShardWriter sw = tableWriter.getShard(new ShardId(tenant, table, interval.getInterval(), interval.getIntervalStart(timestamp), shard));
      if (sw == null)
        return null;
      else
        return sw.getMetadata(columnId);
    }
  }

  @Override
  public void close() {
    if (selfPool) {
      try {
        threadPool.shutdown();
      } catch (Exception e) {
        LOGGER.warn("Unable to close pool");
      }
    }

    for (TableWriter table : tableWriters.values()) {
      try {
        table.close();
      } catch (Exception e) {
        LOGGER.warn("Unable to close table {}", table.getTableName(), e);
      }
    }
  }

  public void delete(String tenant, String table, Interval interval, Instant timestamp, Object entityId, long version, String instanceId) {
    if (transaction == null)
      throw new XactError(null, "No transaction was given, forget to call begin?");
    ShardId shardId = store.findShardId(tenant, table, interval, timestamp, entityId);
    TableId tableId = new TableId(tenant, table);
    TableWriter tableWriter = tableWriters.get(tableId);
    if (tableWriter != null) {
      // This occurs if a write happened first then delete.
      IShardWriter sw = tableWriter.getShard(shardId);
      if (sw != null) {
        sw.delete(entityId, version, instanceId);
        return;
      } else {
        // NOTE: Let it fall through, since its a new shard we haven't loaded yet.
      }
    }

    // If it is null then table doesn't exist yet which means we can just return.
    if (!store.tableExists(tenant, table))
      return;

    // TableMeta is not null then table does exist, in that case load it up and attempt a delete.
    if (tableWriter == null) {
      tableWriter = getTableWriter(tableId);
    }
    tableEntityColumnIds.put(tableId, store.getEntityIdColumn(tenant, table));

    IShardWriter sw = tableWriter.getShard(shardId);
    if (sw == null) {
      IShardWriter sw1 = new ShardWriter(shardId, store, compress, compactionTrigger, captureWrites);
      sw1.begin(transaction);
      sw = tableWriter.addShard(sw1);
    }
    sw.delete(entityId, version, instanceId);
  }
  
  public synchronized TableWriter getTableWriter(TableId tableId) {
    TableWriter existing = tableWriters.get(tableId);
    if (existing == null) {
      TableWriter tableWriter = new TableWriter(tableId.getTenant(), tableId.getTableName());
      tableWriters.put(tableId, tableWriter);
      return tableWriter;
    } else {
      return existing;
    }
  }
  
  /**
   * Builds an initial tablemetadata that defines the table's intended id and type from an Entity object.
   * 
   * @param The entity object to build the entity column id.
   */
  private ColumnId buildEntityColumnId(Entity entity) {
    Object entityId = entity.getEntityId();
    DataType entityIdType = DataType.INTEGER;
    if (entityId instanceof String)
      entityIdType = DataType.STRING;
    String entityIdColumn = entity.getEntityIdColumn();
    return new ColumnId(entityIdColumn, entityIdType.getCode());
  }
  
  public void snapshotCurrentToInterval(String tenant, String table, Interval interval, Instant timestamp) {
    List<ShardId> shardIdsSrc;
    try {
      shardIdsSrc = store.findShardIds(tenant, table, Interval.SINGLE, Instant.now());
    } catch (Exception exception) {
      return;
    }

    for (ShardId shardIdSrc : shardIdsSrc) {
      store.copyShard(
          new ShardId(
              tenant,
              table,
              interval.getInterval(),
              interval.getIntervalStart(timestamp),
              shardIdSrc.getShardNum()
          ),
          shardIdSrc
      );
    }
  }

  public void copyPreviousIntervalSliceIfNewDestination(String tenant, String table, Interval interval, Instant timestamp) {
    List<ShardId> shardIdsSrc;
    try {
      shardIdsSrc = store.findShardIds(tenant, table, interval, Instant.parse(interval.getIntervalStart(timestamp, -1)));
    } catch (Exception exception) {
      return;
    }

    for (ShardId shardIdSrc : shardIdsSrc) {
      store.copyShard(
          new ShardId(
              tenant,
              table,
              interval.getInterval(),
              interval.getIntervalStart(timestamp),
              shardIdSrc.getShardNum()
          ),
          shardIdSrc
      );
    }
  }

  public void write(String tenant, String table, Interval interval, Instant timestamp, List<Entity> entities) {
    if (transaction == null)
      throw new XactError(null, "No transaction was given, forget to call begin?");
    if (entities == null || entities.isEmpty())
      return;

    HashMap<ShardId, List<Entity>> shardToUpdates = new HashMap<>();
    TableId tableId = new TableId(tenant, table);
    final TableWriter tableWriter;
    if (!tableWriters.containsKey(tableId)) {
      if (store.tableExists(tenant, table)) {
        // The table exists, load it up then
        tableWriter = getTableWriter(tableId);
        
        // Make sure the entityid column hasn't changed.
        ColumnId entityIdColumn = store.getEntityIdColumn(tenant, table);
        if (entities.stream().anyMatch(m -> !m.getEntityIdColumn().equals(entityIdColumn.getName())))
          throw new RuntimeException("Inconsistent entity id column names expected " + entityIdColumn + " but detected an entity that had a different name");
        tableEntityColumnIds.put(tableId, entityIdColumn); 
      } else {
        Entity entity = entities.get(0);
        // No shard metadata exists, create the first shard metadata for this.
        tableWriter = getTableWriter(tableId);
        tableEntityColumnIds.put(tableId, buildEntityColumnId(entity));
      }
    } else {
      tableWriter = tableWriters.get(tableId);
    }
    for (Entity entity : entities) {
      ShardId shardId = store.findShardId(tenant, table, interval, timestamp, entity.getEntityId());
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
              IShardWriter shardWriter = tableWriter.getShard(shardId);
              Thread.currentThread().setName(originalThreadName + "(" + shardId.toString() + ")");
              if (shardWriter == null) {
                IShardWriter sw1 = new ShardWriter(shardId, store, compress, compactionTrigger, captureWrites);
                sw1.begin(transaction);
                shardWriter = tableWriter.addShard(sw1);
              }

              List<Entity> entityUpdates = entry.getValue();
              Map<ColumnId, List<WriteRequest>> columnIdEntityColumns = new HashMap<>();
              for (Entity eu : entityUpdates) {
                Object entityId = eu.getEntityId();
                long version = eu.getVersion();
                String instanceId = eu.getInstanceId();
                for (Column ec : eu.columns()) {
                  WriteRequest internalRequest = new WriteRequest(entityId, version, instanceId, ec);
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
                shardWriter.write(columnId, columns);
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
        if (e.getCause() instanceof EntityIdTypeException)
          throw ((EntityIdTypeException) e.getCause());
        else
          throw new RuntimeException(e);
      }
    }
  }

  public void commit() {
    try {
      for (Map.Entry<TableId, TableWriter> e : tableWriters.entrySet()) {
        persistTable(e.getKey(), e.getValue());
      }
    } finally {
      transaction = null;
    } 
  }
  
  private void persistTable(TableId tableId, TableWriter tableWriter) {
    if (tableWriter == null)
      throw new IllegalStateException("The tablewriter is null for table " + tableId);;
    String tenant = tableId.getTenant();
    String table = tableId.getTableName();
    CompletionService<ShardMetadata> std = new ExecutorCompletionService<>(threadPool);
    ColumnId entityColumnId = tableEntityColumnIds.get(tableId);
    ColumnId storedEntityColumnId = store.getEntityIdColumn(tenant, table);

    if (entityColumnId == null) {
      if (!store.tableExists(tenant, table)) {
        throw new RuntimeException("Unable to determine the entityid column name from store or memory, cannot commit");
      }
      entityColumnId = storedEntityColumnId;
    } else {
      if (storedEntityColumnId == null){
        //Table is initiating, save entity column
        store.saveColumnMetadata(tenant, table, entityColumnId, true);
        storedEntityColumnId = store.getEntityIdColumn(tenant, table);
      }
      if (!entityColumnId.equals(storedEntityColumnId)) {
        throw new RuntimeException("The entity id columns for stored and to be persisted are not the same.");
      }
    }

    int submitted = 0;
    final ColumnId finalEntityColumnId = entityColumnId;
    List<EntityOffsetException> offsetExceptions = new ArrayList<>();
    for (IShardWriter shardWriter : tableWriter.getShardWriters()) {
      std.submit(
          () -> {
            String originalName = Thread.currentThread().getName();
            try {
              Thread.currentThread().setName("shardwriter-" + shardWriter.getShardId());
              MDC.put("tenant_id", tableWriter.getTenant());
              ShardMetadata meta = shardWriter.commit(finalEntityColumnId);
              return meta;
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

    Set<ColumnId> columnIds = new HashSet<>();
    for (int i = 0; i < submitted; ++i) {
      try {
        Future<ShardMetadata> future = std.take();
        ShardMetadata smd = future.get();
        if (smd != null)
            columnIds.addAll(smd.columnIds());
      } catch (InterruptedException | ExecutionException e) {
        // Throw specialized handlers up verses wrapped runtime exceptions
        if (!offsetExceptions.isEmpty()) {
          EntityOffsetException offsetException = offsetExceptions.get(0);
          LOGGER.error("!!!Detected an error on table {} in tenant {}", tableWriter.getTableName(), tableWriter.getTenant(), e);
          LOGGER.error(offsetException.getMessage());
          throw offsetException;
        }
        if (e.getCause() instanceof XactError) {
          throw (XactError) e.getCause();
        }
        throw new RuntimeException(e);
      }
      // At this point in time put each column file into the underlying store.
    }
    store.saveTableMetadata(tenant, table, columnIds, entityColumnId);
  }
}
