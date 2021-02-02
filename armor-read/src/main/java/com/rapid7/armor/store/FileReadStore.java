package com.rapid7.armor.store;

import com.rapid7.armor.Constants;
import com.rapid7.armor.read.SlowArmorShard;
import com.rapid7.armor.read.FastArmorShard;
import com.rapid7.armor.schema.ColumnName;
import com.rapid7.armor.shard.ShardId;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
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

  public FileReadStore(Path path) {
    this.basePath = path;
  }

  private ShardId buildShardId(String org, String table, int shardNum) {
    return new ShardId(shardNum, org, table);
  }

  private ShardId buildShardId(String org, String table, String shardNum) {
    return new ShardId(Integer.parseInt(shardNum), org, table);
  }

  @Override
  public List<ShardId> findShardIds(String org, String table, String columnName) {
    List<ShardId> shardIds = new ArrayList<>();
    for (ShardId shardId : findShardIds(org, table)) {
      Path shardIdPath = Paths.get(resolveCurrentPath(shardId.getOrg(), shardId.getTable(), shardId.getShardNum()));
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(shardIdPath)) {
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
  public ShardId findShardId(String org, String table, int shardNum) {
    ShardId shardId = buildShardId(org, table, shardNum);
    Path shardIdPath = basePath.resolve(Paths.get(shardId.getShardId()));
    if (Files.exists(shardIdPath))
      return shardId;
    else
      return null;
  }

  @Override
  public SlowArmorShard getArmorShard(ShardId shardId, String columnName) {
    List<ColumnName> columnNames = getColumNames(shardId);
    Optional<ColumnName> option = columnNames.stream().filter(c -> c.getName().equals(columnName)).findFirst();
    ColumnName cn = option.get();
    Path shardIdPath = Paths.get(resolveCurrentPath(shardId.getOrg(), shardId.getTable(), shardId.getShardNum()), cn.fullName());
    try {
      if (!Files.exists(shardIdPath)) {
        Files.createDirectories(shardIdPath.getParent());
        return new SlowArmorShard();
      } else {
        return new SlowArmorShard(
            new DataInputStream(Files.newInputStream(shardIdPath, StandardOpenOption.READ)));
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public List<ColumnName> getColumNames(ShardId shardId) {
    Path shardIdPath = Paths.get(resolveCurrentPath(shardId.getOrg(), shardId.getTable(), shardId.getShardNum()));
    List<ColumnName> fileList = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(shardIdPath)) {
      for (Path path : stream) {
        if (!Files.isDirectory(path) && !path.getFileName().toString().contains(Constants.SHARD_METADATA)) {
          fileList.add(new ColumnName(path.getFileName().toString()));
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

    return fileList;
  }

  @Override
  public FastArmorShard getFastArmorShard(ShardId shardId, String columName) {
    List<ColumnName> columnNames = getColumNames(shardId);
    Optional<ColumnName> option = columnNames.stream().filter(c -> c.getName().equals(columName)).findFirst();
    ColumnName cn = option.get();
    Path shardIdPath = Paths.get(resolveCurrentPath(shardId.getOrg(), shardId.getTable(), shardId.getShardNum()), cn.fullName());
    if (!Files.exists(shardIdPath)) {
      return null;
    } else {
      try {
        return new FastArmorShard(
            new DataInputStream(Files.newInputStream(shardIdPath, StandardOpenOption.READ)));
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  @Override
  public List<String> getTables(String org) {
    Path orgPath = basePath.resolve(Paths.get(org));
    List<String> tables = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(orgPath)) {
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
  public List<ColumnName> getColumNames(String org, String table) {
    List<ShardId> shardIds = findShardIds(org, table);
    if (shardIds.isEmpty())
      return new ArrayList<>();
    return getColumNames(shardIds.get(0));
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
  @SuppressWarnings("unchecked")
  public Map<String, String> getCurrentValues(String org, String table, int shardNum) {
    Path searchpath = basePath.resolve(Paths.get(org, table, Integer.toString(shardNum), Constants.CURRENT));
    if (!Files.exists(searchpath))
      return new HashMap<>();
    else {
      ObjectMapper mapper = new ObjectMapper();
      try {
        return mapper.readValue(Files.newInputStream(searchpath), Map.class);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  @Override
  public List<String> getOrgs() {
    File[] directories = basePath.toFile().listFiles(File::isDirectory);
    return Arrays.stream(directories).map(File::getName).collect(Collectors.toList());
  }
}
