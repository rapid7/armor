package com.rapid7.armor.store;

import com.rapid7.armor.Constants;
import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.read.fast.FastArmorShardColumn;
import com.rapid7.armor.read.slow.SlowArmorShardColumn;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.shard.ShardId;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class FileReadStore implements ReadStore {
  private final Path basePath;
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public FileReadStore(Path path) {
    this.basePath = path;
  }

  private ShardId buildShardId(String tenant, String table, int shardNum) {
    return new ShardId(shardNum, tenant, table);
  }

  private ShardId buildShardId(String tenant, String table, String shardNum) {
    return new ShardId(Integer.parseInt(shardNum), tenant, table);
  }

  @Override
  public List<ShardId> findShardIds(String tenant, String table, String columnId) {
    List<ShardId> shardIds = new ArrayList<>();
    for (ShardId shardId : findShardIds(tenant, table)) {
      Path shardIdPath = Paths.get(resolveCurrentPath(shardId.getTenant(), shardId.getTable(), shardId.getShardNum()));
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(shardIdPath)) {
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
  public List<ShardId> findShardIds(String tenant, String table) {
    Path searchpath = basePath.resolve(Paths.get(tenant, table));
    Set<ShardId> fileList = new HashSet<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(searchpath)) {
      for (Path path : stream) {
        if (Files.isDirectory(path)) {
          fileList.add(buildShardId(tenant, table, path.getFileName().toString()));
        }
      }
    } catch (NoSuchFileException nfe) {
      return new ArrayList<>();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    return new ArrayList<>(fileList);
  }


  @Override
  public ShardId findShardId(String tenant, String table, int shardNum) {
    ShardId shardId = buildShardId(tenant, table, shardNum);
    Path shardIdPath = basePath.resolve(Paths.get(shardId.getShardId()));
    if (Files.exists(shardIdPath))
      return shardId;
    else
      return null;
  }

  @Override
  public SlowArmorShardColumn getSlowArmorShard(ShardId shardId, String columnId) {
    List<ColumnId> columnIds = getColumnIds(shardId);
    Optional<ColumnId> option = columnIds.stream().filter(c -> c.getName().equals(columnId)).findFirst();
    if (!option.isPresent())
      return null;
    ColumnId cn = option.get();
    Path shardIdPath = Paths.get(resolveCurrentPath(shardId.getTenant(), shardId.getTable(), shardId.getShardNum()), cn.fullName());
    try {
      if (!Files.exists(shardIdPath)) {
        Files.createDirectories(shardIdPath.getParent());
        return new SlowArmorShardColumn();
      } else {
        return new SlowArmorShardColumn(
            new DataInputStream(Files.newInputStream(shardIdPath, StandardOpenOption.READ)));
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public List<ColumnId> getColumnIds(ShardId shardId) {
    Path shardIdPath = Paths.get(resolveCurrentPath(shardId.getTenant(), shardId.getTable(), shardId.getShardNum()));
    List<ColumnId> fileList = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(shardIdPath)) {
      for (Path path : stream) {
        if (!Files.isDirectory(path) && !path.getFileName().toString().contains(Constants.SHARD_METADATA)) {
          fileList.add(new ColumnId(path.getFileName().toString()));
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

    return fileList;
  }

  @Override
  public FastArmorShardColumn getFastArmorShard(ShardId shardId, String columnName) {
    List<ColumnId> columnIds = getColumnIds(shardId);
    Optional<ColumnId> option = columnIds.stream().filter(c -> c.getName().equals(columnName)).findFirst();
    if (!option.isPresent())
      return null;
    ColumnId cn = option.get();
    Path shardIdPath = Paths.get(resolveCurrentPath(shardId.getTenant(), shardId.getTable(), shardId.getShardNum()), cn.fullName());
    if (!Files.exists(shardIdPath)) {
      return null;
    } else {
      try {
        return new FastArmorShardColumn(new DataInputStream(Files.newInputStream(shardIdPath, StandardOpenOption.READ)));
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  @Override
  public List<String> getTables(String tenant) {
    Path tenantPath = basePath.resolve(Paths.get(tenant));
    List<String> tables = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(tenantPath)) {
      for (Path path : stream) {
        if (Files.isDirectory(path)) {
          tables.add(path.getFileName().toString());
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    return tables;
  }

  @Override
  public List<ColumnId> getColumnIds(String tenant, String table) {
    List<ShardId> shardIds = findShardIds(tenant, table);
    if (shardIds.isEmpty())
      return new ArrayList<>();
    Set<ColumnId> columnIds = new HashSet<>();
    for (ShardId shardId : shardIds)
      columnIds.addAll(getColumnIds(shardId));
    return new ArrayList<>(columnIds);
  }

  @Override
  public String resolveCurrentPath(String tenant, String table, int shardNum) {
    Map<String, String> values = getCurrentValues(tenant, table, shardNum);
    String current = values.get("current");
    if (current == null)
      return null;
    return basePath.resolve(Paths.get(tenant, table, Integer.toString(shardNum), current)).toString();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<String, String> getCurrentValues(String tenant, String table, int shardNum) {
    Path searchpath = basePath.resolve(Paths.get(tenant, table, Integer.toString(shardNum), Constants.CURRENT));
    if (!Files.exists(searchpath))
      return new HashMap<>();
    else {
      try {
        return OBJECT_MAPPER.readValue(Files.newInputStream(searchpath), Map.class);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  @Override
  public List<String> getTenants() {
    File[] directories = basePath.toFile().listFiles(File::isDirectory);
    return Arrays.stream(directories).map(File::getName).collect(Collectors.toList());
  }

  @Override
  public ColumnId findColumnId(String tenant, String table, String columnName) {
    List<ColumnId> columnIds = getColumnIds(tenant, table);
    Optional<ColumnId> first = columnIds.stream().filter(c -> c.getName().equalsIgnoreCase(columnName)).findFirst();
    if (first.isPresent())
      return first.get();
    else
      return null;
  }

  @Override
  public ShardMetadata getShardMetadata(String tenant, String table, int shardNum) {
    String currendPath = resolveCurrentPath(tenant, table, shardNum);
    if (currendPath == null)
      return null;
    Path shardIdPath = basePath.resolve(Paths.get(currendPath, Constants.SHARD_METADATA + ".armor"));
    if (!Files.exists(shardIdPath))
      return null;
    try {
      byte[] payload = Files.readAllBytes(shardIdPath);
      return OBJECT_MAPPER.readValue(payload, ShardMetadata.class);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}
