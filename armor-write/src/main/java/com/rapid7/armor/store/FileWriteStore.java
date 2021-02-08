package com.rapid7.armor.store;

import com.rapid7.armor.Constants;
import com.rapid7.armor.entity.Entity;
import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.meta.TableMetadata;
import com.rapid7.armor.schema.ColumnName;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.shard.ShardStrategy;
import com.rapid7.armor.write.ColumnFileWriter;
import com.rapid7.armor.write.WriteRequest;
import com.fasterxml.jackson.core.type.TypeReference;
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
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileWriteStore implements WriteStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileWriteStore.class);
  private final Path basePath;
  private final ShardStrategy shardStrategy;

  public FileWriteStore(Path path, ShardStrategy shardStrategy) {
    this.basePath = path;
    this.shardStrategy = shardStrategy;
  }

  public ShardId buildShardId(String org, String table, int shardNum) {
    return new ShardId(shardNum, org, table);
  }

  private ShardId buildShardId(String org, String table, String num) {
    return new ShardId(Integer.parseInt(num), org, table);
  }

  @Override
  public List<ShardId> findShardIds(String org, String table, String columnName) {
    List<ShardId> shardIds = new ArrayList<>();
    for (ShardId shardId : findShardIds(org, table)) {
      Path currentPath = Paths.get(resolveCurrentPath(org, table, shardId.getShardNum()));
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(currentPath)) {
        for (Path path : stream) {
          if (!Files.isDirectory(path)) {
            if (path.getFileName().toString().startsWith(columnName))
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
  public List<ShardId> findShardIds(String org, String table) {
    Path searchpath = basePath.resolve(Paths.get(org, table));
    Set<ShardId> fileList = new HashSet<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(searchpath)) {
      for (Path path : stream) {
        if (Files.isDirectory(path)) {
          fileList.add(buildShardId(org, table, path.getFileName().toString()));
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    return new ArrayList<>(fileList);
  }

  @Override
  public ShardId findShardId(String org, String table, Object entityId) {
    int shardNum = shardStrategy.shardNum(entityId);
    return buildShardId(org, table, shardNum);
  }

  @Override
  public void saveColumn(String transaction, ColumnShardId columnShardId, int byteSize, InputStream inputStream) {
    Path shardIdPath = basePath.resolve(Paths.get(columnShardId.getShardId().getShardId(), transaction, columnShardId.getColumnName().fullName()));
    try {
      Files.createDirectories(shardIdPath.getParent());
      long copied = Files.copy(inputStream, shardIdPath, StandardCopyOption.REPLACE_EXISTING);
      if (copied != byteSize) {
        LOGGER.warn("Expected to write {} but confirmed only {} bytes were copied", byteSize, copied);
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public ColumnFileWriter loadColumnWriter(ColumnShardId columnShardId) {
    String currentPath = resolveCurrentPath(columnShardId.getOrg(), columnShardId.getTable(), columnShardId.getShardNum());
    if (currentPath == null) {
      try {
        return new ColumnFileWriter(columnShardId);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    Path shardIdPath = basePath.resolve(Paths.get(currentPath, columnShardId.getColumnName().fullName()));
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
  public List<ColumnName> getColumNames(ShardId shardId) {
    String currentPath = resolveCurrentPath(shardId.getOrg(), shardId.getTable(), shardId.getShardNum());
    if (currentPath == null)
      return new ArrayList<>();
    Path target = Paths.get(currentPath);
    Set<ColumnName> fileList = new HashSet<>();
    try {
      Files.createDirectories(target);
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(target)) {
        for (Path path : stream) {
          if (!Files.isDirectory(path) && !path.getFileName().toString().contains(Constants.SHARD_METADATA)) {
            fileList.add(new ColumnName(path.getFileName().toString()));
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
  public List<ColumnFileWriter> loadColumnWriters(String org, String table, int shardNum) {
    String currentPath = resolveCurrentPath(org, table, shardNum);
    ShardId shardId = buildShardId(org, table, shardNum);
    List<ColumnName> columNames = getColumNames(buildShardId(org, table, shardNum));
    List<ColumnFileWriter> writers = new ArrayList<>();
    TableMetadata tableMetadata = this.loadTableMetadata(org, table);
    for (ColumnName columnName : columNames) {
      if (tableMetadata.getEntityColumnId().equals(columnName.getName()))
        continue;
      Path shardIdPath = basePath.resolve(Paths.get(currentPath, columnName.fullName()));
      try {
        if (!Files.exists(shardIdPath)) {
          Files.createDirectories(shardIdPath.getParent());
          writers.add(new ColumnFileWriter(new ColumnShardId(shardId, columnName)));
        } else {
          ColumnFileWriter writer = new ColumnFileWriter(new DataInputStream(Files.newInputStream(shardIdPath, StandardOpenOption.READ)), new ColumnShardId(shardId, columnName));
          writers.add(writer);
        }
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    return writers;
  }

  @Override
  public TableMetadata loadTableMetadata(String org, String table) {
    String relativeTarget = org + "/" + table + "/" + Constants.TABLE_METADATA + ".armor";
    Path target = basePath.resolve(relativeTarget);
    if (!Files.exists(target))
      return null;
    ObjectMapper mapper = new ObjectMapper();
    try {
      byte[] payload = Files.readAllBytes(target);
      return mapper.readValue(payload, TableMetadata.class);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void saveTableMetadata(String transaction, String org, String table, TableMetadata tableMetadata) {
    String relativeTarget = org + "/" + table + "/" + Constants.TABLE_METADATA + ".armor";
    Path target = basePath.resolve(relativeTarget);
    ObjectMapper mapper = new ObjectMapper();
    try {
      byte[] payload = mapper.writeValueAsBytes(tableMetadata);
      if (!Files.exists(target)) {
        Files.createDirectories(target.getParent());
        Files.write(target, payload, StandardOpenOption.CREATE_NEW);
      } else
        Files.write(target, payload, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public ShardMetadata loadShardMetadata(String org, String table, int shardNum) {
    String currendPath = resolveCurrentPath(org, table, shardNum);
    if (currendPath == null)
      return null;
    Path shardIdPath = basePath.resolve(Paths.get(currendPath, Constants.SHARD_METADATA + ".armor"));
    if (!Files.exists(shardIdPath))
      return null;
    ObjectMapper mapper = new ObjectMapper();
    try {
      byte[] payload = Files.readAllBytes(shardIdPath);
      return mapper.readValue(payload, ShardMetadata.class);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void saveShardMetadata(String transcationId, String org, String table, int shardNum, ShardMetadata shardMetadata) {
    ShardId shardId = buildShardId(org, table, shardNum);
    Path shardIdPath = basePath.resolve(Paths.get(shardId.getShardId(), transcationId, Constants.SHARD_METADATA + ".armor"));
    try {
      Files.createDirectories(shardIdPath.getParent());
      ObjectMapper om = new ObjectMapper();
      byte[] payload = om.writeValueAsBytes(shardMetadata);
      Files.copy(new ByteArrayInputStream(payload), shardIdPath, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void commit(String transaction, String org, String table, int shardNum) {
    Map<String, String> currentValues = getCurrentValues(org, table, shardNum);
    String oldCurrent = null;
    final String oldPrevious;
    if (currentValues != null) {
      oldCurrent = currentValues.get("current");
      oldPrevious = currentValues.get("previous");
    } else
      oldPrevious = null;
    if (oldCurrent != null && oldCurrent.equalsIgnoreCase(transaction))
      throw new WriteTranscationError("Create another transaction", transaction);
    saveCurrentValues(org, table, shardNum, transaction, oldCurrent);
    try {
      Runnable runnable = () -> {
        try {
          if (oldPrevious == null)
            return;
          Path toDelete = basePath.resolve(Paths.get(org, table, Integer.toString(shardNum), oldPrevious));
          Files.walk(toDelete)
              .sorted(Comparator.reverseOrder())
              .map(Path::toFile)
              .forEach(File::delete);
        } catch (IOException ioe) {
          LOGGER.warn("Unable to previous shard version under {}", oldPrevious, ioe);
        }
      };
      Thread t = new Thread(runnable);
      t.start();
    } catch (Exception e) {
      LOGGER.warn("Unable to previous shard version under {}", oldPrevious, e);
    }
  }

  @Override
  public String resolveCurrentPath(String org, String table, int shardNum) {
    Map<String, String> values = getCurrentValues(org, table, shardNum);
    String current = values.get("current");
    if (current == null)
      return null;
    return basePath.resolve(Paths.get(org, table, Integer.toString(shardNum), current)).toString();
  }

  @Override
  public Map<String, String> getCurrentValues(String org, String table, int shardNum) {
    Path searchpath = basePath.resolve(Paths.get(org, table, Integer.toString(shardNum), Constants.CURRENT));
    if (!Files.exists(searchpath))
      return new HashMap<>();
    else {
      ObjectMapper mapper = new ObjectMapper();
      try {
        return mapper.readValue(Files.newInputStream(searchpath), new TypeReference<Map<String, String>>() {});
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  @Override
  public void saveCurrentValues(String org, String table, int shardNum, String current, String previous) {
    Path searchpath = basePath.resolve(Paths.get(org, table, Integer.toString(shardNum), Constants.CURRENT));
    try {
      Files.createDirectories(searchpath.getParent());
      HashMap<String, String> currentValues = new HashMap<>();
      currentValues.put("current", current);
      if (previous != null)
        currentValues.put("previous", previous);
      ObjectMapper mapper = new ObjectMapper();
      Files.write(searchpath, mapper.writeValueAsBytes(currentValues), StandardOpenOption.CREATE);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void rollback(String transaction, String org, String table, int shardNum) {
    try {
      Path toDelete = basePath.resolve(Paths.get(org, table, Integer.toString(shardNum), transaction));
      Files.walk(toDelete)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    } catch (IOException ioe) {
      LOGGER.warn("Unable to previous shard version under {}", transaction, ioe);
    }
  }

  @Override
  public void saveError(String transaction, ColumnShardId columnShardId, int size, InputStream inputStream, String error) {

    Path toDelete = basePath.resolve(
        Paths.get(columnShardId.getOrg(), columnShardId.getTable(), Integer.toString(columnShardId.getShardNum()), Constants.LAST_ERROR));
    try {
      Files.walk(toDelete)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .filter(f -> !f.getName().contains(transaction))
          .forEach(File::delete);
    } catch (IOException ioe) {
      LOGGER.warn("Unable to previous shard version under {}", toDelete, ioe);
    }

    Path shardIdPath = basePath.resolve(Paths.get(
        columnShardId.getOrg(),
        columnShardId.getTable(),
        Integer.toString(columnShardId.getShardNum()),
        Constants.LAST_ERROR,
        transaction,
        columnShardId.getColumnName().fullName()));
    try {
      Files.createDirectories(shardIdPath.getParent());
      long copied = Files.copy(inputStream, shardIdPath, StandardCopyOption.REPLACE_EXISTING);
      if (copied != size) {
        LOGGER.warn("Expected to write {} but confirmed only {} bytes were copied", size, copied);
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void captureWrites(String correlationId, ShardId shardId, List<Entity> entities, List<WriteRequest> requests, Object deleteEntity) {
  }
}
