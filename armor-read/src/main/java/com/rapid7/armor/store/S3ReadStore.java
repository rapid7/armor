package com.rapid7.armor.store;

import com.rapid7.armor.Constants;
import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.read.fast.FastArmorShardColumn;
import com.rapid7.armor.read.predicate.InstantPredicate;
import com.rapid7.armor.read.predicate.StringPredicate;
import com.rapid7.armor.read.slow.SlowArmorShardColumn;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.io.PathBuilder;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.xact.DistXactRecord;
import com.rapid7.armor.xact.DistXactRecordUtil;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.rapid7.armor.Constants.COLUMN_METADATA_DIR;

public class S3ReadStore implements ReadStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3ReadStore.class);
  private final AmazonS3 s3Client;
  private final String bucket;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public S3ReadStore(AmazonS3 s3Client, String bucket) {
    this.s3Client = s3Client;
    this.bucket = bucket;
  }

  @Override
  public List<ShardId> findShardIds(String tenant, String table, Interval interval, Instant timestamp) {
    ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucket).withMaxKeys(10000);
    lor.withDelimiter(Constants.STORE_DELIMETER);
    lor.withPrefix(getIntervalPrefix(tenant, table, interval, timestamp) + Constants.STORE_DELIMETER);
    ListObjectsV2Result ol;
    // Remove trailing /
    Set<ShardId> shards = new HashSet<>();
    do {
      ol = s3Client.listObjectsV2(lor);
      List<String> commonPrefixes = ol.getCommonPrefixes();
      List<String> rawShardNames = commonPrefixes.stream().map(cp -> cp.substring(0, cp.length() - 1)).collect(Collectors.toList());
      shards.addAll(rawShardNames.stream().map(s -> toShardId(tenant, table, interval, timestamp, s)).collect(Collectors.toList()));
      lor.setContinuationToken(ol.getNextContinuationToken());
    } while (ol.isTruncated());
    return new ArrayList<>(shards);
  }

  @Override
  public List<ShardId> findShardIds(String tenant, String table, Interval interval, Instant timestamp, String columnId) {
    ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucket).withMaxKeys(10000);
    lor.withDelimiter(Constants.STORE_DELIMETER);
    lor.withPrefix(getIntervalPrefix(tenant, table, interval, timestamp) + Constants.STORE_DELIMETER);
    ListObjectsV2Result ol;
    // Remove trailing /
    Set<ShardId> shards = new HashSet<>();
    do {
      ol = s3Client.listObjectsV2(lor);
      List<String> commonPrefixes = ol.getCommonPrefixes();
      List<String> rawShardNames = commonPrefixes.stream().map(cp -> cp.substring(0, cp.length() - 1)).collect(Collectors.toList());
      shards.addAll(rawShardNames.stream().map(s -> toShardId(tenant, table, interval, timestamp, s)).collect(Collectors.toList()));
      lor.setContinuationToken(ol.getNextContinuationToken());
    } while (ol.isTruncated());
    return new ArrayList<>(shards);
  }

  @Override
  public boolean shardIdExists(ShardId shardId) {
    ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucket).withMaxKeys(10000);
    lor.withDelimiter(Constants.STORE_DELIMETER);
    lor.withPrefix(shardId.shardIdPath() + Constants.STORE_DELIMETER);
    ListObjectsV2Result ol;
    do {
      ol = s3Client.listObjectsV2(lor);
      if (ol.getCommonPrefixes().stream().anyMatch(s -> s.contains(shardId.shardIdPath())))
        return true;
      lor.setContinuationToken(ol.getNextContinuationToken());
    } while (ol.isTruncated());
    return false;
  }

  @Override
  public SlowArmorShardColumn getSlowArmorShard(ShardId shardId, String columnName) {
    List<ColumnId> columnIds = getColumnIds(shardId);
    Optional<ColumnId> option = columnIds.stream().filter(c -> c.getName().equals(columnName)).findFirst();
    if (!option.isPresent())
      return null;
    ColumnId cn = option.get();
    String shardIdPath = PathBuilder.buildPath(resolveCurrentPath(shardId), cn.fullName());
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
  public FastArmorShardColumn getFastArmorShard(ShardId shardId, String columnName) {
    List<ColumnId> columnIds = getColumnIds(shardId);
    Optional<ColumnId> option = columnIds.stream().filter(c -> c.getName().equals(columnName)).findFirst();
    if (!option.isPresent())
      return null;
    ColumnId cn = option.get();
    String shardIdPath = PathBuilder.buildPath(resolveCurrentPath(shardId), cn.fullName());
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
    String columnMetadataPath = PathBuilder.buildPath(tenant, table, COLUMN_METADATA_DIR);
  
    ListObjectsV2Request lor = new ListObjectsV2Request()
        .withBucketName(bucket)
        .withDelimiter(Constants.STORE_DELIMETER)
        .withPrefix(columnMetadataPath + Constants.STORE_DELIMETER);
    
    Set<ColumnId> columnIds = new HashSet<>();
    ListObjectsV2Result ol;
    do {
      ol = s3Client.listObjectsV2(lor);
      List<S3ObjectSummary> summaries = ol.getObjectSummaries();
      columnIds.addAll(summaries.stream()
          .map(objectSummary -> Paths.get(objectSummary.getKey()).getFileName().toString().substring(1))
          .map(ColumnId::new)
          .collect(Collectors.toList())
      );
      lor.setContinuationToken(ol.getNextContinuationToken());
    } while (ol.isTruncated());
    return new ArrayList<>(columnIds);
  }

  @Override
  public List<ColumnId> getColumnIds(String tenant, String table, Interval interval, Instant timestamp) {
    List<ShardId> shardIds = findShardIds(tenant, table, interval, timestamp);
    if (shardIds.isEmpty())
      return new ArrayList<>();
    return getColumnIds(shardIds.get(0));
  }
  
  @Override
  public List<ColumnId> getColumnIds(ShardId shardId) {
    ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucket).withMaxKeys(10000);
    lor.withDelimiter(Constants.STORE_DELIMETER);
    lor.withPrefix(resolveCurrentPath(shardId) + Constants.STORE_DELIMETER);
    HashSet<ColumnId> columnIds = new HashSet<>();
    ListObjectsV2Result ol;
    do {
      ol = s3Client.listObjectsV2(lor);
      List<S3ObjectSummary> summaries = ol.getObjectSummaries();
      columnIds.addAll(summaries.stream()
        .map(s -> Paths.get(s.getKey()).getFileName().toString())
        .filter(n -> !n.contains(Constants.SHARD_METADATA))
        .map(ColumnId::new).collect(Collectors.toList()));
      lor.setContinuationToken(ol.getNextContinuationToken());
    } while (ol.isTruncated());
    return new ArrayList<>(columnIds);
  }

  @Override
  public List<String> getTables(String tenant) {
    ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucket).withMaxKeys(10000);
    lor.withDelimiter(Constants.STORE_DELIMETER);
    lor.withPrefix(tenant + Constants.STORE_DELIMETER);
    ListObjectsV2Result ol;
    List<String> tables = new ArrayList<>();
    do {
      ol = s3Client.listObjectsV2(lor);
      List<String> commonPrefixes = ol.getCommonPrefixes();
      tables.addAll(commonPrefixes.stream().map(cp -> cp.substring(0, cp.length() - 1)).map(cp -> cp.replace(tenant + Constants.STORE_DELIMETER, "")).collect(Collectors.toList()));
      lor.setContinuationToken(ol.getNextContinuationToken());
    } while (ol.isTruncated());
    return tables;
  }

  @Override
  public List<String> getTenants(boolean useCache) {
    if (useCache) {
      List<String> tenantsInCache = listTenantsFromCache();
      return tenantsInCache.isEmpty() ? listTenantsFromBucket() : tenantsInCache;
    } else {
      return listTenantsFromBucket();
    }
  }

  private List<String> listTenantsFromBucket() {
    ListObjectsV2Request lor = new ListObjectsV2Request()
        .withBucketName(bucket)
        .withMaxKeys(10000)
        .withDelimiter(Constants.STORE_DELIMETER);
    
    Set<String> allPrefixes = new HashSet<>();
    ListObjectsV2Result result;
    do {
      result = s3Client.listObjectsV2(lor);
      allPrefixes.addAll(result.getCommonPrefixes().stream().map(o -> o.replace(Constants.STORE_DELIMETER, "")).collect(Collectors.toList()));
      lor.setContinuationToken(result.getNextContinuationToken());
    } while (result.isTruncated());
    List<String> tenants = allPrefixes.stream().filter(t -> !t.startsWith(StoreConstants.TENANT_EXCLUDE_FILTER_PREFIX)).collect(Collectors.toList());
    tenants.forEach(this::trackTenant);
    return tenants;
  }
  
  private List<String> listTenantsFromCache() {
    ListObjectsV2Request lor = new ListObjectsV2Request()
        .withBucketName(bucket)
        .withPrefix(StoreConstants.TENANT_CACHE_DIR)
        .withMaxKeys(10000);
    
    Set<String> orgs = new HashSet<>();
    ListObjectsV2Result result;
    do {
      result = s3Client.listObjectsV2(lor);
      for (S3ObjectSummary summary : result.getObjectSummaries()) {
        orgs.add(Paths.get(summary.getKey()).getFileName().toString());
      }
      lor.setContinuationToken(result.getNextContinuationToken());
    } while (result.isTruncated());
    return orgs.stream().filter(t -> !t.startsWith(StoreConstants.TENANT_EXCLUDE_FILTER_PREFIX)).collect(Collectors.toList());
  }
  
  @Override
  public ColumnId getColumnId(String tenant, String table, Interval interval, Instant timestamp, String columnName) {
    List<ColumnId> columnIds = getColumnIds(tenant, table, interval, timestamp);
    Optional<ColumnId> first = columnIds.stream().filter(c -> c.getName().equalsIgnoreCase(columnName)).findFirst();
    return first.orElse(null);
  }

  @Override
  public ShardMetadata getShardMetadata(ShardId shardId) {
    String shardIdPath = PathBuilder.buildPath(resolveCurrentPath(shardId), Constants.SHARD_METADATA + ".armor");

    if (s3Client.doesObjectExist(bucket, shardIdPath)) {
      try (S3Object s3Object = s3Client.getObject(bucket, shardIdPath); S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent()) {
        try {
          return OBJECT_MAPPER.readValue(s3ObjectInputStream, ShardMetadata.class);
        } finally {
          com.amazonaws.util.IOUtils.drainInputStream(s3ObjectInputStream);
        }
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    } else
      return null;
  }
  
  private String getIntervalPrefix(String tenant, String table, Interval interval, Instant timestamp) {
    return PathBuilder.buildPath(tenant, table, interval.getInterval(), interval.getIntervalStart(timestamp));
  }

  private String resolveCurrentPath(ShardId shardId) {
    DistXactRecord status = getCurrentValues(shardId);
    if (status == null || status.getCurrent() == null)
      return null;
    return PathBuilder.buildPath(shardId.shardIdPath(), status.getCurrent());
  }

  private DistXactRecord getCurrentValues(ShardId shardId) {
    String key = DistXactRecordUtil.buildCurrentMarker(shardId.shardIdPath());
    if (!doesObjectExist(this.bucket, key))
      return null;
    else {
      try (S3Object s3Object = s3Client.getObject(bucket, key); S3ObjectInputStream inputStream = s3Object.getObjectContent()) {
        return DistXactRecordUtil.readXactStatus(inputStream);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }
  
  private ShardId toShardId(String tenant, String table, Interval interval, Instant timestamp, String rawShard) {
    String shardName = Paths.get(rawShard).getFileName().toString();
    int shardNum = Integer.parseInt(shardName);
    return ShardId.buildShardId(tenant, table, interval, timestamp, shardNum);
  }

  @Override
  public List<String> getIntervalStarts(String tenant, String table, Interval interval) {
    ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucket).withMaxKeys(10000);
    lor.withDelimiter(Constants.STORE_DELIMETER);
    String prefix = PathBuilder.buildPath(tenant, table, interval.getInterval()) + Constants.STORE_DELIMETER;
    lor.withPrefix(prefix);
    ListObjectsV2Result ol;
    // Remove trailing /
    Set<String> intervalStarts = new HashSet<>();
    do {
      ol = s3Client.listObjectsV2(lor);
      List<String> commonPrefixes = ol.getCommonPrefixes();
      intervalStarts.addAll(
        commonPrefixes.stream().map(cp -> cp.substring(0, cp.length() - 1).replaceAll(prefix, "")).collect(Collectors.toList()));
      lor.setContinuationToken(ol.getNextContinuationToken());
    } while (ol.isTruncated());
    return new ArrayList<>(intervalStarts);
  }

  @Override
  public List<String> getIntervalStarts(String tenant, String table, Interval interval, InstantPredicate intervalStart) {
    List<String> intervalStarts = getIntervalStarts(tenant, table, interval);
    List<Instant> instants = intervalStarts.stream().map(is -> Instant.parse(is)).collect(Collectors.toList());
    List<String> matches = new ArrayList<>();
    for (Instant instant : instants) {
      if (intervalStart == null || intervalStart.test(instant))
        matches.add(instant.toString());
    }
    return matches;
  }
  
  @Override
  public List<ShardId> findShardIds(String tenant, String table, Interval interval) {
      if (interval == Interval.SINGLE)
          return findShardIds(tenant, table, interval, Instant.now());
      List<String> matches = getIntervalStarts(tenant, table, interval);
      List<ShardId> shardIds = new ArrayList<>();
      for (String match : matches) {
          shardIds.addAll(findShardIds(tenant, table, interval, Instant.parse(match)));
      }
      return shardIds;
  }
  
  @Override
  public List<ShardId> findShardIds(String tenant, String table, Interval interval, InstantPredicate intervalStartPredicate) {
    if (interval != null && interval == Interval.SINGLE)
      return findShardIds(tenant, table, interval, Instant.now());
    Map<Interval, List<String>> intervalStarts = new HashMap<>();
    if (interval == null) {
      for (Interval inter : getIntervals(tenant, table))
        intervalStarts.put(inter, getIntervalStarts(tenant, table, inter));
    } else 
        intervalStarts.put(interval, getIntervalStarts(tenant, table, interval));
      
    Set<ShardId> shardIds = new HashSet<>();
    for (Map.Entry<Interval, List<String>> entry : intervalStarts.entrySet()) {
      List<String> intervalStartsValues = entry.getValue();
      List<Instant> intervalStartInstances = new ArrayList<>();
      for (String isv : intervalStartsValues) {
         Instant instant = Instant.parse(isv);
         intervalStartInstances.add(instant);
      }
      List<String> matches = new ArrayList<>();
      for (Instant intervalStartInstant : intervalStartInstances) {
        if (intervalStartPredicate == null || intervalStartPredicate.test(intervalStartInstant))
          matches.add(intervalStartInstant.toString());
      }
       
      // So now we have the matching intervals, next for each interval get the shardIds
      for (String match : matches) {
        shardIds.addAll(findShardIds(tenant, table, entry.getKey(), Instant.parse(match)));
      }
    }
    return new ArrayList<>(shardIds);
  }
  
  @Override
  public List<ShardId> findShardIds(String tenant, String table, StringPredicate interval, InstantPredicate intervalStartPredicate) {
      if (interval == null) {
          // This is gonna be slow but we will do it.
          List<ShardId> shardIds = new ArrayList<>();
          List<Interval> intervals = getIntervals(tenant, table);
          for (Interval inter : intervals) {
              shardIds.addAll(findShardIds(tenant, table, inter, intervalStartPredicate));
          }
          return shardIds;
      } else if (interval.getOperator() == Operator.EQUALS && interval.getValue().equalsIgnoreCase(Interval.SINGLE.getInterval()))
          return findShardIds(tenant, table, Interval.SINGLE, Instant.now());

      List<Interval> intervals = getIntervals(tenant, table);
      List<ShardId> shardIds = new ArrayList<>();
      for (Interval inter : intervals) {
          if (inter != null && interval.test(inter.getInterval()))
              shardIds.addAll(findShardIds(tenant, table, inter, intervalStartPredicate));
      }
      return shardIds;
  }

  @Override
  public List<Interval> getIntervals(String tenant, String table) {
    ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucket).withMaxKeys(10000);
    lor.withPrefix(PathBuilder.buildPath(tenant, table) + Constants.STORE_DELIMETER);
    lor.withDelimiter(Constants.STORE_DELIMETER);

    Set<String> allPrefixes = new HashSet<>();
    ListObjectsV2Result result;
    do {
      result = s3Client.listObjectsV2(lor);
      allPrefixes.addAll(
          result.getCommonPrefixes().stream()
          .map(o -> Paths.get(o).getFileName().toString()).collect(Collectors.toList()));
      lor.setContinuationToken(result.getNextContinuationToken());
    } while (result.isTruncated());
    return allPrefixes.stream().map(i -> Interval.toInterval(i)).filter(i -> i != null).collect(Collectors.toList());
  }
  
  private String resolveCurrentPath(String tenant, String table) {
    DistXactRecord status = getCurrentValues(tenant, table);
    if (status == null || status.getCurrent() == null)
      return null;
    return PathBuilder.buildPath(tenant, table, status.getCurrent());
  }
  
  private DistXactRecord getCurrentValues(String tenant, String table) {
    String key = DistXactRecordUtil.buildCurrentMarker(PathBuilder.buildPath(tenant, table));
    if (!doesObjectExist(this.bucket, key))
      return null;
    else {
      try (S3Object s3Object = s3Client.getObject(bucket, key); S3ObjectInputStream inputStream = s3Object.getObjectContent()) {
        return DistXactRecordUtil.readXactStatus(inputStream);
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
        if (i == 10) {
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
  
  private void trackTenant(String tenant) {
    String key = PathBuilder.buildPath(StoreConstants.TENANT_CACHE_DIR, tenant);
    InputStream inputStream = new ByteArrayInputStream(new byte[0]);
    ObjectMetadata metadata = new ObjectMetadata();
    metadata.setContentLength(0L);
    
    s3Client.putObject(new PutObjectRequest(bucket, key, inputStream, metadata));
  }
}
