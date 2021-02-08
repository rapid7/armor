package com.rapid7.armor.store;

import com.rapid7.armor.Constants;
import com.rapid7.armor.read.fast.FastArmorShardColumn;
import com.rapid7.armor.read.slow.SlowArmorShardColumn;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.shard.ShardId;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3ReadStore implements ReadStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3ReadStore.class);
  private final AmazonS3 s3Client;
  private final String bucket;

  public S3ReadStore(AmazonS3 s3Client, String bucket) {
    this.s3Client = s3Client;
    this.bucket = bucket;
  }

  private ShardId buildShardId(String tenant, String table, int shardNum) {
    return new ShardId(shardNum, tenant, table);
  }

  @Override
  public List<ColumnId> getColumnIds(ShardId shardId) {
    ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucket).withMaxKeys(10000);
    lor.withDelimiter("/");
    lor.withPrefix(resolveCurrentPath(shardId.getTenant(), shardId.getTable(), shardId.getShardNum()) + "/");
    ListObjectsV2Result ol = s3Client.listObjectsV2(lor);
    List<S3ObjectSummary> summaries = ol.getObjectSummaries();
    return summaries.stream()
        .map(s -> Paths.get(s.getKey()).getFileName().toString())
        .filter(n -> !n.contains(Constants.SHARD_METADATA))
        .map(ColumnId::new).collect(Collectors.toList());
  }

  @Override
  public List<ShardId> findShardIds(String tenant, String table) {
    ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucket).withMaxKeys(10000);
    lor.withDelimiter("/");
    lor.withPrefix(tenant + "/" + table + "/");
    ListObjectsV2Result ol = s3Client.listObjectsV2(lor);
    List<String> commonPrefixes = ol.getCommonPrefixes();
    // Remove trailing /
    List<String> rawShardNames = commonPrefixes.stream().map(cp -> cp.substring(0, cp.length() - 1)).collect(Collectors.toList());
    return rawShardNames.stream().map(s -> toShardId(tenant, table, s)).collect(Collectors.toList());
  }

  private ShardId toShardId(String tenant, String table, String rawShard) {
    String shardName = Paths.get(rawShard).getFileName().toString();
    int shardNum = Integer.parseInt(shardName);
    return buildShardId(tenant, table, shardNum);
  }

  @Override
  public List<ShardId> findShardIds(String tenant, String table, String columnId) {
    ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucket).withMaxKeys(10000);
    lor.withDelimiter("/");
    lor.withPrefix(tenant + "/" + table + "/");
    ListObjectsV2Result ol = s3Client.listObjectsV2(lor);
    List<String> commonPrefixes = ol.getCommonPrefixes();
    // Remove trailing /
    List<String> rawShardNames = commonPrefixes.stream().map(cp -> cp.substring(0, cp.length() - 1)).collect(Collectors.toList());
    return rawShardNames.stream().map(s -> toShardId(tenant, table, s)).collect(Collectors.toList());
  }

  @Override
  public ShardId findShardId(String tenant, String table, int shardNum) {
    ShardId shardId = buildShardId(tenant, table, shardNum);
    ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucket).withMaxKeys(10000);
    lor.withDelimiter("/");
    lor.withPrefix(tenant + "/" + table + "/");
    ListObjectsV2Result ol = s3Client.listObjectsV2(lor);
    List<String> commonPrefixes = ol.getCommonPrefixes();

    List<String> rawShardNames = commonPrefixes.stream().map(cp -> cp.substring(0, cp.length() - 1)).collect(Collectors.toList());
    if (rawShardNames.stream().map(s -> toShardId(tenant, table, s)).anyMatch(s -> s.equals(shardId))) {
      return shardId;
    } else
      return null;
  }

  @Override
  public SlowArmorShardColumn getSlowArmorShard(ShardId shardId, String columnId) {
    List<ColumnId> columnIds = getColumnIds(shardId);
    Optional<ColumnId> option = columnIds.stream().filter(c -> c.getName().equals(columnId)).findFirst();
    ColumnId cn = option.get();
    String shardIdPath = resolveCurrentPath(shardId.getTenant(), shardId.getTable(), shardId.getShardNum()) + "/" + cn.fullName();
    if (!doesObjectExist(bucket, shardIdPath)) {
      return new SlowArmorShardColumn();
    } else {
      S3Object s3Object = s3Client.getObject(bucket, shardIdPath);
      try {
        return new SlowArmorShardColumn(new DataInputStream(s3Object.getObjectContent()));
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  @Override
  public FastArmorShardColumn getFastArmorShard(ShardId shardId, String columnId) {
    List<ColumnId> columnIds = getColumnIds(shardId);
    Optional<ColumnId> option = columnIds.stream().filter(c -> c.getName().equals(columnId)).findFirst();
    ColumnId cn = option.get();
    String shardIdPath = resolveCurrentPath(shardId.getTenant(), shardId.getTable(), shardId.getShardNum()) + "/" + cn.fullName();
    if (!doesObjectExist(bucket, shardIdPath)) {
      return null;
    } else {
      S3Object s3Object = s3Client.getObject(bucket, shardIdPath);
      try {
        return new FastArmorShardColumn(new DataInputStream(s3Object.getObjectContent()));
      } catch (IOException ioe) {
        LOGGER.error("Unable load the shard at {}", shardIdPath, ioe);
        throw new RuntimeException(ioe);
      }
    }
  }

  @Override
  public List<ColumnId> getColumnIds(String tenant, String table) {
    List<ShardId> shardIds = findShardIds(tenant, table);
    if (shardIds.isEmpty())
      return new ArrayList<>();
    return getColumnIds(shardIds.get(0));
  }

  @Override
  public List<String> getTables(String tenant) {
    ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucket).withMaxKeys(10000);
    lor.withDelimiter("/");
    lor.withPrefix(tenant + "/");
    ListObjectsV2Result ol = s3Client.listObjectsV2(lor);
    List<String> commonPrefixes = ol.getCommonPrefixes();
    return commonPrefixes.stream().map(cp -> cp.substring(0, cp.length() - 1)).map(cp -> cp.replace(tenant + "/", "")).collect(Collectors.toList());
  }

  @Override
  public String resolveCurrentPath(String tenant, String table, int shardNum) {
    Map<String, String> values = getCurrentValues(tenant, table, shardNum);
    String current = values.get("current");
    if (current == null)
      return null;
    return tenant + "/" + table + "/" + shardNum + "/" + current;
  }

  @Override
  public Map<String, String> getCurrentValues(String tenant, String table, int shardNum) {
    String key = tenant + "/" + table + "/" + shardNum + "/" + Constants.CURRENT;
    if (!doesObjectExist(this.bucket, key))
      return new HashMap<>();
    else {
      ObjectMapper mapper = new ObjectMapper();
      try (S3Object s3Object = s3Client.getObject(bucket, key); S3ObjectInputStream inputStream = s3Object.getObjectContent()) {
        return mapper.readValue(inputStream, new TypeReference<Map<String, String>>() {});
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  /**
   * Attempts exists check, if its errors out it is most likely a slowdown error. So sleep for a second and retry again.
   */
  private boolean doesObjectExist(String bucket, String key) {
    for (int i = 0; i < 10; i++) {
      try {
        return s3Client.doesObjectExist(bucket, key);
      } catch (AmazonS3Exception e) {
        if (i == 2) {
          throw e;
        }
        try {
          Thread.sleep((i + 1) * 1000);
        } catch (InterruptedException ie) {
          // do nothing
        }
      }
    }
    throw new IllegalStateException("Should not have dropped into this section");
  }

  @Override
  public List<String> getTenants() {
    ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucket).withMaxKeys(10000);
    lor.withDelimiter("/");
    ListObjectsV2Result ol = s3Client.listObjectsV2(lor);
    List<String> commonPrefixes = ol.getCommonPrefixes();
    return commonPrefixes.stream().map(o -> o.replace("/", "")).collect(Collectors.toList());
  }
}
