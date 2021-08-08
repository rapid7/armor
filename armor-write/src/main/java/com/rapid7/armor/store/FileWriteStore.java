package com.rapid7.armor.store;

import com.rapid7.armor.Constants;
import com.rapid7.armor.columnfile.ColumnFileReader;
import com.rapid7.armor.entity.Entity;
import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.io.PathBuilder;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.shard.ShardStrategy;
import com.rapid7.armor.write.WriteRequest;
import com.rapid7.armor.write.writers.ColumnFileWriter;
import com.rapid7.armor.xact.ArmorXact;
import com.rapid7.armor.xact.DistXactRecord;
import com.rapid7.armor.xact.DistXactRecordUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.rapid7.armor.Constants.COLUMN_METADATA_DIR;
import static com.rapid7.armor.schema.ColumnId.keyName;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class FileWriteStore implements WriteStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileWriteStore.class);
  private final Path basePath;
  private final ShardStrategy shardStrategy;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public FileWriteStore(Path path, ShardStrategy shardStrategy) {
    this.basePath = path;
    this.shardStrategy = shardStrategy;
  }

  private ShardId buildShardId(String tenant, String table, Interval interval, Instant timestamp, String num) {
    return new ShardId(tenant, table, interval.getInterval(), interval.getIntervalStart(timestamp), Integer.parseInt(num));
  }

  @Override
  public List<ShardId> findShardIds(String tenant, String table, Interval interval, Instant timestamp, String columnId) {
    List<ShardId> shardIds = new ArrayList<>();
    for (ShardId shardId : findShardIds(tenant, table, interval, timestamp)) {
      Path currentPath = Paths.get(resolveCurrentPath(shardId));
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(currentPath)) {
        for (Path path : stream) {
          if (!Files.isDirectory(path)) {
            if (path.getFileName().toString().startsWith(columnId))
              shardIds.add(shardId);
          }
        }
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    return shardIds;
  }

  @Override
  public List<ShardId> findShardIds(String tenant, String table, Interval interval, Instant timestamp) {
    Path searchPath = basePath.resolve(Paths.get(tenant, table, interval.getInterval(), interval.getIntervalStart(timestamp)));
    Set<ShardId> fileList = new HashSet<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(searchPath)) {
      for (Path path : stream) {
        if (Files.isDirectory(path)) {
          fileList.add(buildShardId(tenant, table, interval, timestamp, path.getFileName().toString()));
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    return new ArrayList<>(fileList);
  }

  @Override
  public ShardId findShardId(String tenant, String table, Interval interval, Instant timestamp, Object entityId) {
    int shardNum = shardStrategy.shardNum(entityId);
    return ShardId.buildShardId(tenant, table, interval, timestamp, shardNum);
  }

  @Override
  public void saveColumn(ArmorXact armorTransaction, ColumnShardId columnShardId, int byteSize, InputStream inputStream) {
    Path shardIdPath = basePath.resolve(Paths.get(columnShardId.getShardId().shardIdPath(), armorTransaction.getTarget(), columnShardId.getColumnId().fullName()));
    try {
      Files.createDirectories(shardIdPath.getParent());
      long copied = Files.copy(inputStream, shardIdPath, REPLACE_EXISTING);
      if (copied != byteSize) {
        LOGGER.warn("Expected to write {} but confirmed only {} bytes were copied", byteSize, copied);
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public ColumnFileWriter loadColumnWriter(ColumnShardId columnShardId) {
    String currentPath = resolveCurrentPath(columnShardId.getShardId());
    if (currentPath == null) {
      try {
        return new ColumnFileWriter(columnShardId);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    Path shardIdPath = basePath.resolve(Paths.get(currentPath, columnShardId.getColumnId().fullName()));
    try {
      if (!Files.exists(shardIdPath)) {
        Files.createDirectories(shardIdPath.getParent());
        return new ColumnFileWriter(columnShardId);
      } else {
        return new ColumnFileWriter(new DataInputStream(Files.newInputStream(shardIdPath, StandardOpenOption.READ)), columnShardId);
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public List<ColumnId> getColumnIds(ShardId shardId) {
    String currentPath = resolveCurrentPath(shardId);
    if (currentPath == null)
      return new ArrayList<>();
    Path target = Paths.get(currentPath);
    Set<ColumnId> fileList = new HashSet<>();
    try {
      Files.createDirectories(target);
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(target)) {
        for (Path path : stream) {
          if (!Files.isDirectory(path) && !path.getFileName().toString().contains(Constants.SHARD_METADATA)) {
            fileList.add(new ColumnId(path.getFileName().toString()));
          }
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    return new ArrayList<>(fileList);
  }

  @Override
  public int findShardNum(Object entityId) {
    return shardStrategy.shardNum(entityId);
  }

  @Override
  public List<ColumnFileWriter> loadColumnWriters(ShardId shardId) {
    String currentPath = resolveCurrentPath(shardId);
    List<ColumnId> columnIds = getColumnIds(shardId);
    List<ColumnFileWriter> writers = new ArrayList<>();
    for (ColumnId columnId : columnIds) {
      Path shardIdPath = basePath.resolve(Paths.get(currentPath, columnId.fullName()));
      try {
        if (Files.exists(shardIdPath)) {
          ColumnFileWriter writer = new ColumnFileWriter(new DataInputStream(Files.newInputStream(shardIdPath, StandardOpenOption.READ)), new ColumnShardId(shardId, columnId));
          if (writer.getMetadata().getEntityId()) {
            writer.close();
            continue;
          }
          writers.add(writer);
        }
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    return writers;
  }

  @Override
  public void saveTableMetadata(String tenant, String table, Set<ColumnId> columnIds, ColumnId entityColumnId) {
    saveColumnMetadata(tenant, table, entityColumnId, true);
    for (ColumnId column : columnIds) {
      if (entityColumnId.equals(column))
          continue;
      saveColumnMetadata(tenant, table, column, false);
    }
  }

  @Override
  public void saveColumnMetadata(String tenant, String table, ColumnId column, boolean isEntityColumn) {
    String columnFile = PathBuilder.buildPath(
        tenant,
        table,
        COLUMN_METADATA_DIR,
        keyName(column, isEntityColumn)
    );
  
    File file = basePath.resolve(columnFile).toFile();
    try {
      if (!file.exists()) {
        Files.createDirectories(file.getParentFile().toPath());
        file.createNewFile();
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
  
  @Override
  public ShardMetadata getShardMetadata(ShardId shardId) {
    String currentPath = resolveCurrentPath(shardId);
    if (currentPath == null)
      return null;
    Path shardIdPath = basePath.resolve(Paths.get(currentPath, Constants.SHARD_METADATA + ".armor"));
    if (!Files.exists(shardIdPath))
      return null;
    try {
      byte[] payload = Files.readAllBytes(shardIdPath);
      return OBJECT_MAPPER.readValue(payload, ShardMetadata.class);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void saveShardMetadata(ArmorXact transaction, ShardMetadata shardMetadata) {
    ShardId shardId = shardMetadata.getShardId();
    Path shardIdPath = basePath.resolve(Paths.get(shardId.shardIdPath(), transaction.getTarget(), Constants.SHARD_METADATA + ".armor"));
    try {
      Files.createDirectories(shardIdPath.getParent());
      byte[] payload = OBJECT_MAPPER.writeValueAsBytes(shardMetadata);
      Files.copy(new ByteArrayInputStream(payload), shardIdPath, REPLACE_EXISTING);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void copyShard(ShardId shardIdDst, ShardId shardIdSrc) {
    if (shardIdDst.equals(shardIdSrc)) {
      return;
    }

    File shardIdDstDirectory = new File(basePath.resolve(Paths.get(shardIdDst.shardIdPath())).toString());
    if (shardIdDstDirectory.exists()) {
      return;
    }

    File shardIdSrcDirectory = new File(basePath.resolve(Paths.get(shardIdSrc.shardIdPath())).toString());
    if (!shardIdSrcDirectory.exists() || !shardIdSrcDirectory.isDirectory()) {
      return;
    }

    File copying = new File(basePath.resolve(Paths.get(shardIdDst.shardIdPath(), "COPYING")).toString());
    try {
      Files.createDirectories(shardIdDstDirectory.toPath());
      copying.createNewFile();
      copyDirectory(shardIdSrcDirectory.toPath(), shardIdDstDirectory.toPath());
    } catch (Exception exception) {
      if (shardIdDstDirectory.exists()) {
        deleteDirectory(shardIdDstDirectory.toPath());
      }

      throw new RuntimeException(exception);
    } finally {
      if (copying.exists()) {
        copying.delete();
      }
    }
  }

  @Override
  public void commit(ArmorXact armorTransaction, ShardId shardId) {
    DistXactRecord status = getCurrentValues(shardId);
    DistXactRecordUtil.validateXact(status, armorTransaction);
    saveCurrentValues(shardId, new DistXactRecord(armorTransaction, status));
    try {
      Runnable runnable = () -> {
        try {
          if (status == null || status.getPrevious() == null)
            return;
          Path toDelete = basePath.resolve(Paths.get(shardId.shardIdPath(), status.getPrevious()));
          Files.walk(toDelete)
              .sorted(Comparator.reverseOrder())
              .map(Path::toFile)
              .forEach(File::delete);
        } catch (IOException ioe) {
          LOGGER.warn("Unable to previous shard version under {}", status.getPrevious(), ioe);
        }
      };
      Thread t = new Thread(runnable);
      t.start();
    } catch (Exception e) {
      LOGGER.warn("Unable to previous shard version under {}", status.getPrevious(), e);
    }
  }

  @Override
  public void rollback(ArmorXact transaction, ShardId shardId) {
    try {
      Path toDelete = basePath.resolve(Paths.get(shardId.shardIdPath(), transaction.getTarget()));
      Files.walk(toDelete)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    } catch (IOException ioe) {
      LOGGER.warn("Unable to previous shard version under {}", transaction, ioe);
    }
  }
  
  @Override
  public List<String> getTenants(boolean useCache) {
    File[] directories = basePath.toFile().listFiles(File::isDirectory);
    return Arrays.stream(directories).map(File::getName).collect(Collectors.toList());
  }

  @Override
  public String saveError(ArmorXact transaction, ColumnShardId columnShardId, int size, InputStream inputStream, String error) {

    Path toDelete = basePath.resolve(
        Paths.get(columnShardId.getTenant(), columnShardId.getTable(), columnShardId.getInterval(), columnShardId.getIntervalStart(), Integer.toString(columnShardId.getShardNum()), Constants.LAST_ERROR));
    try {
      Files.walk(toDelete)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .filter(f -> !f.getName().contains(transaction.getTarget()))
          .forEach(File::delete);
    } catch (IOException ioe) {
      LOGGER.warn("Unable to previous shard version under {}", toDelete, ioe);
    }

    Path shardIdPath = basePath.resolve(Paths.get(
        columnShardId.getTenant(),
        columnShardId.getTable(),
        columnShardId.getInterval(),
        columnShardId.getIntervalStart(),
        Integer.toString(columnShardId.getShardNum()),
        Constants.LAST_ERROR,
        transaction.getTarget(),
        columnShardId.getColumnId().fullName()));
    try {
      Files.createDirectories(shardIdPath.getParent());
      long copied = Files.copy(inputStream, shardIdPath, REPLACE_EXISTING);
      if (copied != size) {
        LOGGER.warn("Expected to write {} but confirmed only {} bytes were copied", size, copied);
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    return shardIdPath.toString();
  }

  @Override
  public void captureWrites(ArmorXact armorTransaction, ShardId shardId, List<Entity> entities, List<WriteRequest> requests, Object deleteEntity) {
  }

  @Override
  public String rootDirectory() {
    return basePath.toString();
  }

  @Override
  public void deleteTenant(String tenant) {
    try {
      Path toDelete = basePath.resolve(Paths.get(tenant));
      Files.walk(toDelete)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    } catch (Exception e) {
      LOGGER.warn("Unable completely remove tenant {}", tenant, e);
    }
  }

  @Override
  public ColumnMetadata getColumnMetadata(String tenant, String table, ColumnShardId columnShardId) {
    String currentPath = resolveCurrentPath(columnShardId.getShardId());
    if (currentPath == null)
      return null;
    Path shardIdPath = basePath.resolve(Paths.get(currentPath, columnShardId.getColumnId().fullName()));
    try {
      if (!Files.exists(shardIdPath)) {
        return null;
      } else {
        ColumnFileReader reader = new ColumnFileReader();
        reader.read(new DataInputStream(Files.newInputStream(shardIdPath, StandardOpenOption.READ)), null);
        return reader.getColumnMetadata();
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private String resolveCurrentPath(ShardId shardId) {
    DistXactRecord status = getCurrentValues(shardId);
    if (status == null || status.getCurrent() == null)
      return null;
    return basePath.resolve(Paths.get(shardId.shardIdPath(), status.getCurrent())).toString();
  }

  private DistXactRecord getCurrentValues(String tenant, String table) {
    Path searchPath = basePath.resolve(DistXactRecordUtil.buildCurrentMarker(Paths.get(tenant, table).toString()));
    if (!Files.exists(searchPath))
      return null;
    else {
      try (InputStream is = Files.newInputStream(searchPath)) {
        return DistXactRecordUtil.readXactRecord(is);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  private DistXactRecord getCurrentValues(ShardId shardId) {
    Path searchPath = basePath.resolve(DistXactRecordUtil.buildCurrentMarker(Paths.get(shardId.shardIdPath()).toString()));
    if (!Files.exists(searchPath))
      return null;
    else {
      try (InputStream is = Files.newInputStream(searchPath)) {
        return DistXactRecordUtil.readXactRecord(is);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  public void saveCurrentValues(String tenant, String table, String current, String previous) {
    Path searchPath = basePath.resolve(DistXactRecordUtil.buildCurrentMarker(Paths.get(tenant, table).toString()));
    try {
      Files.createDirectories(searchPath.getParent());
      HashMap<String, String> currentValues = new HashMap<>();
      currentValues.put("current", current);
      if (previous != null)
        currentValues.put("previous", previous);
      Files.write(searchPath, OBJECT_MAPPER.writeValueAsBytes(currentValues), StandardOpenOption.CREATE);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  public void saveCurrentValues(ShardId shardId, DistXactRecord transaction) {
    Path searchPath = basePath.resolve(DistXactRecordUtil.buildCurrentMarker(Paths.get(shardId.shardIdPath()).toString()));
    try {
      Files.createDirectories(searchPath.getParent());
      HashMap<String, Object> currentValues = new HashMap<>();
      currentValues.put("current", transaction.getCurrent());
      currentValues.put("currentTime", transaction.getCurrentTime());

      if (transaction.getPrevious() != null)
        currentValues.put("previous", transaction.getPrevious());
      if (transaction.getPreviousTime() != null)
        currentValues.put("previousTime", transaction.getPreviousTime());
      Files.write(searchPath, OBJECT_MAPPER.writeValueAsBytes(currentValues), StandardOpenOption.CREATE);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private void copyDirectory(Path src, Path dst) {
    try (Stream<Path> paths = Files.walk(src)) {
      AtomicReference<Path> current = new AtomicReference<>();
      paths.filter(file -> !src.equals(file)).forEach(
          file -> {
            try {
              if (file.endsWith(DistXactRecord.CURRENT_MARKER)) {
                current.set(file);
              } else {
                Files.copy(file, dst.resolve(src.relativize(file)), REPLACE_EXISTING);
              }
            } catch (IOException exception) {
              throw new RuntimeException(exception);
            }
          }
      );
      if (current.get() != null) {
        Files.copy(current.get(), dst.resolve(src.relativize(current.get())), REPLACE_EXISTING);
      }
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
  }

  private void deleteDirectory(Path directory) {
    try (Stream<Path> paths = Files.walk(directory)) {
      paths.forEach(
          source -> {
            try {
              Files.delete(source);
            } catch (IOException exception) {
              throw new RuntimeException(exception);
            }
          }
      );
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
  }

  @Override
  public void deleteTable(String tenant, String table) {
    try {
      Path toDelete = basePath.resolve(Paths.get(tenant, table));
      Files.walk(toDelete)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    } catch (Exception e) {
      LOGGER.warn("Unable completely remove tenant {}", tenant, e);
    }
  }

  @Override
  public void deleteInterval(String tenant, String table, Interval interval) {
    try {
      Path toDelete = basePath.resolve(Paths.get(tenant, table, interval.getInterval()));
      Files.walk(toDelete)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    } catch (Exception e) {
      LOGGER.warn("Unable completely remove tenant {}", tenant, e);
    }    
  }

  @Override
  public void deleteIntervalStart(String tenant, String table, Interval interval, String intervalStart) {
    try {
      Path toDelete = basePath.resolve(Paths.get(tenant, table, interval.getInterval(), intervalStart));
      Files.walk(toDelete)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    } catch (Exception e) {
      LOGGER.warn("Unable completely remove tenant {}", tenant, e);
    }
  }

  @Override
  public boolean intervalExists(String tenant, String table, Interval interval) {
    Path intervalPath = basePath.resolve(Paths.get(tenant, table, interval.getInterval()));
    return Files.exists(intervalPath);
  }

  @Override
  public boolean tableExists(String tenant, String table) {
    Path intervalPath = basePath.resolve(Paths.get(tenant, table));
    return Files.exists(intervalPath);
  }

  @Override
  public boolean intervalStartExists(String tenant, String table, Interval interval, String intervalStart) {
    Path intervalStartPath = basePath.resolve(Paths.get(tenant, table, interval.getInterval(), intervalStart));
    return Files.exists(intervalStartPath);
  }

  @Override
  public boolean columnShardIdExists(ColumnShardId columnShardId) {
    String currentPath = resolveCurrentPath(columnShardId.getShardId());
    Path shardIdPath = basePath.resolve(Paths.get(currentPath, columnShardId.getColumnId().fullName()));
    return Files.exists(shardIdPath);
  }

  @Override
  public ColumnId getEntityIdColumn(String tenant, String table) {
    File metadataDirectory = basePath.resolve(PathBuilder.buildPath(tenant, table, COLUMN_METADATA_DIR)).toFile();
    File[] metadataFiles = metadataDirectory.listFiles((dir, name) -> name.startsWith(ColumnId.ENTITY_COLUMN_IDENTIFIER));
  
    if (metadataFiles != null && metadataFiles.length > 0) {
      String columnFullName = metadataFiles[0].getName().substring(1);
      return new ColumnId(columnFullName);
    }
    return null;
  }

  @Override
  public ArmorXact begin(String transaction, ShardId shardId) {
      if (transaction == null)
          throw new IllegalArgumentException("No transaction was given");
      DistXactRecord xact = getCurrentValues(shardId);
      
      // Special case: First one wins scenario. Since no previous transaction exists start the process
      // of claiming it by saving a current first then building another transaction.
      if (xact == null) {
          String baselineTransaction = UUID.randomUUID().toString();
          xact = new DistXactRecord(baselineTransaction, System.currentTimeMillis(), null, null);
          saveCurrentValues(shardId, xact);
      }
      return DistXactRecord.generateNewTransaction(transaction, xact);
  }
}
