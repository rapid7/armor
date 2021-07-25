package com.rapid7.armor.store;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
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
import com.amazonaws.ResetException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.util.StringInputStream;
import com.amazonaws.util.StringUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.rapid7.armor.Constants.COLUMN_METADATA_DIR;
import static com.rapid7.armor.schema.ColumnId.keyName;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class S3WriteStore implements WriteStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3WriteStore.class);
  private final AmazonS3 s3Client;
  private final String bucket;
  private final ShardStrategy shardStrategy;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String INTERVAL_TAG = "interval";
  private static final String ARCHIVING_MARKER = "ARCHIVING";
  private static final Set<String> tenantCache = new HashSet<>();
  private static final Cache<String, Set<String>> columnCache = CacheBuilder.newBuilder()
      .maximumSize(1_000_000)
      .concurrencyLevel(10)
      .initialCapacity(200_000)
      .expireAfterAccess(7, TimeUnit.DAYS).build();
  private Executor tpool;

  public void setThreadPool(int threads) {
    this.tpool = Executors.newFixedThreadPool(threads);
  }

  public S3WriteStore(AmazonS3 s3Client, String bucket, ShardStrategy shardStrategy) {
    this.s3Client = s3Client;
    this.bucket = bucket;
    this.shardStrategy = shardStrategy;
    this.tpool = Executors.newFixedThreadPool(10);
  }

  @Override
  public ShardId findShardId(String tenant, String table, Interval interval, Instant timestamp, Object entityId) {
    int shardNum = shardStrategy.shardNum(entityId);
    return ShardId.buildShardId(tenant, table, interval, timestamp, shardNum);
  }

  @Override
  public void saveColumn(ArmorXact armorTransaction, ColumnShardId columnShardId, int byteSize, InputStream inputStream) {
    String key = PathBuilder.buildPath(columnShardId.getShardId().shardIdPath(), armorTransaction.getTarget(), columnShardId.getColumnId().fullName());
    ObjectMetadata omd = new ObjectMetadata();
    omd.setContentLength(byteSize);
    try {
      putObject(key, inputStream, omd, columnShardId.getInterval());
    } catch (ResetException e) {
      LOGGER.error("Detected a reset exception, the number of bytes is {}: {}", byteSize, e.getExtraInfo());
      throw e;
    }
  }

  @Override
  public ColumnFileWriter loadColumnWriter(ColumnShardId columnShardId) {
    String shardIdPath = PathBuilder.buildPath(resolveCurrentPath(columnShardId.getShardId()), columnShardId.getColumnId().fullName());
    try {
      if (!s3Client.doesObjectExist(bucket, shardIdPath)) {
        return new ColumnFileWriter(columnShardId);
      } else {
        try (S3Object s3Object = s3Client.getObject(bucket, shardIdPath); S3ObjectInputStream s3ObjectInputSTream = s3Object.getObjectContent()) {
          try {
            return new ColumnFileWriter(new DataInputStream(s3Object.getObjectContent()), columnShardId);
          } finally {
            com.amazonaws.util.IOUtils.drainInputStream(s3ObjectInputSTream);
          }
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public List<ColumnId> getColumnIds(ShardId shardId) {
    ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucket).withMaxKeys(10000);
    lor.withDelimiter(Constants.STORE_DELIMETER);
    lor.withPrefix(resolveCurrentPath(shardId) + Constants.STORE_DELIMETER);
    Set<ColumnId> columnIds = new HashSet<>();
    ListObjectsV2Result ol;
    do {
      ol = s3Client.listObjectsV2(lor);
      List<S3ObjectSummary> summaries = ol.getObjectSummaries();
      columnIds.addAll(summaries.stream()
          .map(s -> Paths.get(s.getKey()).getFileName().toString())
          .filter(n -> !n.contains(Constants.SHARD_METADATA))
          .map(ColumnId::new).collect(toList()));
      lor.setContinuationToken(ol.getNextContinuationToken());
    } while (ol.isTruncated());
    return new ArrayList<>(columnIds);
  }

  @Override
  public List<ShardId> findShardIds(String tenant, String table, Interval interval, Instant timestamp) {
    ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucket).withMaxKeys(10000);
    lor.withDelimiter(Constants.STORE_DELIMETER);
    lor.withPrefix(PathBuilder.buildPath(tenant, table, interval.getInterval(), interval.getIntervalStart(timestamp)) + Constants.STORE_DELIMETER);
    ListObjectsV2Result ol;
    Set<ShardId> shardIds = new HashSet<>();
    do {
      ol = s3Client.listObjectsV2(lor);
      List<String> commonPrefixes = ol.getCommonPrefixes();
      List<String> rawShardNames = commonPrefixes.stream().map(cp -> cp.substring(0, cp.length() - 1)).collect(toList());
      shardIds.addAll(rawShardNames.stream().map(s -> toShardId(tenant, table, interval, timestamp, s)).collect(toSet()));
      lor.setContinuationToken(ol.getNextContinuationToken());
    } while (ol.isTruncated());
    return new ArrayList<>(shardIds);
  }

  @Override
  public List<ShardId> findShardIds(String tenant, String table, Interval interval, Instant timestamp, String columnId) {
    ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucket).withMaxKeys(10000);
    lor.withDelimiter(Constants.STORE_DELIMETER);
    lor.withPrefix(PathBuilder.buildPath(tenant, table, interval.getInterval(), interval.getIntervalStart(timestamp)) + Constants.STORE_DELIMETER);
    ListObjectsV2Result ol;
    Set<ShardId> shardIds = new HashSet<>();
    do {
      ol = s3Client.listObjectsV2(lor);
      List<String> commonPrefixes = ol.getCommonPrefixes();
      // Remove trailing /
      List<String> rawShardNames = commonPrefixes.stream().map(cp -> cp.substring(0, cp.length() - 1)).collect(toList());
      shardIds.addAll(rawShardNames.stream().map(s -> toShardId(tenant, table, interval, timestamp, s)).collect(toList()));
      lor.setContinuationToken(ol.getNextContinuationToken());
    } while (ol.isTruncated());
    return new ArrayList<>(shardIds);
  }

  @Override
  public int findShardNum(Object entityId) {
    return shardStrategy.shardNum(entityId);
  }

  @Override
  public List<ColumnFileWriter> loadColumnWriters(ShardId shardId) {
    List<ColumnId> columnIds = getColumnIds(shardId);
    List<ColumnFileWriter> writers = new ArrayList<>();
    for (ColumnId columnId : columnIds) {
      String shardIdPath = PathBuilder.buildPath(resolveCurrentPath(shardId), columnId.fullName());
      try {
        if (doesObjectExist(bucket, shardIdPath)) {
          S3ObjectInputStream s3InputStream = null;
          try (S3Object s3Object = s3Client.getObject(bucket, shardIdPath);) {
            s3InputStream = s3Object.getObjectContent();
            ColumnFileWriter writer = new ColumnFileWriter(new DataInputStream(s3InputStream), new ColumnShardId(shardId, columnId));
            if (writer.getMetadata().getEntityId()) {
              writer.close();
              continue;
            }
            writers.add(writer);
          } catch (Exception e) {
            LOGGER.error("Detected an issue loading shard at {}, this investigate", shardIdPath, e);
            throw e;
          } finally {
            if (s3InputStream != null)
              com.amazonaws.util.IOUtils.drainInputStream(s3InputStream);
          }
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
    String metadataPath = PathBuilder.buildPath(tenant, table, COLUMN_METADATA_DIR);
    String columnKey = keyName(column, isEntityColumn);
    
    if (!columnExistsInCache(tenant, columnKey)){
      //save column file to s3
      String columnPath = PathBuilder.buildPath(metadataPath, columnKey);
      putObject(columnPath, "", null);
  
      //save column file to cache
      Set<String> cachedColumns = columnCache.getIfPresent(tenant);
      if (cachedColumns == null) {
        cachedColumns = new HashSet<>();
        cachedColumns.add(columnKey);
        columnCache.put(tenant, cachedColumns);
      }
    }
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

  @Override
  public void saveShardMetadata(ArmorXact transaction, ShardMetadata shardMetadata) {
    ShardId shardId = shardMetadata.getShardId();
    String shardIdPath = PathBuilder.buildPath(shardId.shardIdPath(), transaction.getTarget(), Constants.SHARD_METADATA + ".armor");
    for (int i = 0; i < 10; i++) {
      try {
        String payload = OBJECT_MAPPER.writeValueAsString(shardMetadata);
        putObject(shardIdPath, payload, shardId.getInterval());
        break;
      } catch (Exception ioe) {
        if (i + 1 == 10)
          throw new RuntimeException(ioe);
        else {
          try {
            Thread.sleep((i + 1) * 1000);
          } catch (InterruptedException ie) {
            // do nothing
          }
        }
      }
    }
  }

  @Override
  public void copyShard(ShardId shardIdDst, ShardId shardIdSrc) {
    if (shardIdDst.equals(shardIdSrc)) {
      return;
    }

    Path shardDstPath = Paths.get(shardIdDst.shardIdPath());
    ListObjectsV2Result ol = s3Client.listObjectsV2(
        new ListObjectsV2Request()
            .withBucketName(bucket)
            .withMaxKeys(10000)
            .withPrefix(shardDstPath.toString() + Constants.STORE_DELIMETER)
    );
    if (!ol.getObjectSummaries().isEmpty()) {
      return;
    }

    Path shardSrcPath = Paths.get(shardIdSrc.shardIdPath());
    ListObjectsV2Request srcRequest = new ListObjectsV2Request()
      .withBucketName(bucket)
      .withMaxKeys(10000)
      .withPrefix(shardSrcPath.toString() + Constants.STORE_DELIMETER);
    ol = s3Client.listObjectsV2(srcRequest);
    if (ol.getObjectSummaries().isEmpty()) {
      return;
    }

    String currentShardKey = null;
    try {
      List<S3ObjectSummary> currentShardObjects = getCurrentShardObjects(shardIdSrc);
      if (currentShardObjects.isEmpty()) {
        throw new RuntimeException("Expected current shard to contain objects. shard: " + shardIdSrc.shardIdPath());
      }
      
      currentShardKey = getCurrentShardKey(shardSrcPath, currentShardObjects);
      putObject(shardDstPath + "COPYING", "", shardIdDst.getInterval());
      putObject(PathBuilder.buildPath(currentShardKey, ARCHIVING_MARKER), "", shardIdSrc.getInterval());
      ObjectTagging objectTagging = createObjectTagging(shardIdDst.getInterval());
  
      for (S3ObjectSummary objectToCopy : currentShardObjects) {
        s3Client.copyObject(
            new CopyObjectRequest(
                bucket,
                objectToCopy.getKey(),
                bucket,
                shardDstPath.resolve(shardSrcPath.relativize(Paths.get(objectToCopy.getKey()))).toString()
            ).withNewObjectTagging(objectTagging)
        );
      }
      // TODO: Double check.
      String transaction = Paths.get(currentShardKey).getFileName().toString();
      ArmorXact axact = new ArmorXact(transaction, "none", System.currentTimeMillis());
      saveCurrentValues(shardIdDst, new DistXactRecord(axact, null));
      
    } catch (Exception exception) {
      s3Client.listObjectsV2(
          new ListObjectsV2Request()
              .withBucketName(bucket)
              .withMaxKeys(10000)
              .withPrefix(shardDstPath.toString() + Constants.STORE_DELIMETER)
      ).getObjectSummaries().forEach(
          s3ObjectSummary -> s3Client.deleteObject(new DeleteObjectRequest(bucket, s3ObjectSummary.getKey()))
      );

      throw new RuntimeException(exception);
    } finally {
      boolean shardCopyWasAttempted = currentShardKey != null;
      if (shardCopyWasAttempted) {
        s3Client.deleteObject(new DeleteObjectRequest(bucket, shardDstPath + "COPYING"));
        s3Client.deleteObject(new DeleteObjectRequest(bucket, PathBuilder.buildPath(currentShardKey, ARCHIVING_MARKER)));
      }
    }
  }
  
  @Override
  public void commit(ArmorXact armorTransaction, ShardId shardId) {
    DistXactRecord status = getCurrentValues(shardId);
    status.validateXact(armorTransaction);
  
    saveCurrentValues(shardId, new DistXactRecord(armorTransaction, status));
    trackTenant(shardId.getTenant());
    boolean isArchiving = status != null && doesObjectExist(bucket, PathBuilder.buildPath(shardId.shardIdPath(), status.getCurrent(), ARCHIVING_MARKER));
    
    if (!isArchiving) {
      tpool.execute(new Runnable() {
        @Override
        public void run() {
          try {
            if (status == null || status.getPrevious() == null)
              return;
            String toDelete = PathBuilder.buildPath(shardId.shardIdPath(), status.getPrevious());
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(bucket)
                .withPrefix(toDelete);
            ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
            while (true) {
              for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                s3Client.deleteObject(bucket, objectSummary.getKey());
              }
              if (objectListing.isTruncated()) {
                objectListing = s3Client.listNextBatchOfObjects(objectListing);
              } else {
                break;
              }
            }
          } catch (Throwable t) {
            LOGGER.warn("Unable to delete previous shard version under {}", status.getPrevious(), t);
          }
        }
      });
    }
  }

  @Override
  public void rollback(ArmorXact transaction, ShardId shardId) {
    String toDelete = PathBuilder.buildPath(shardId.shardIdPath(), transaction.getTarget());
    try {
      ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
          .withBucketName(bucket)
          .withPrefix(toDelete);
      ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
      while (true) {
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
          s3Client.deleteObject(bucket, objectSummary.getKey());
        }
        if (objectListing.isTruncated()) {
          objectListing = s3Client.listNextBatchOfObjects(objectListing);
        } else {
          break;
        }
      }
    } catch (Exception e) {
      LOGGER.error("Unable to cleanup changes, check {} for proper cleanup", toDelete);
    }
  }

  @Override
  public String saveError(ArmorXact transaction, ColumnShardId columnShardId, int size, InputStream inputStream, String error) {
    // First erase any previous errors that may have existed before.
    String toDelete = PathBuilder.buildPath(columnShardId.getShardId().shardIdPath(), Constants.LAST_ERROR);
    try {
      ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
          .withBucketName(bucket)
          .withPrefix(toDelete);
      ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
      while (true) {
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
          if (!objectSummary.getKey().contains(transaction.getTarget()))
            s3Client.deleteObject(bucket, objectSummary.getKey());
        }
        if (objectListing.isTruncated()) {
          objectListing = s3Client.listNextBatchOfObjects(objectListing);
        } else {
          break;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Unable to previous shard version under {}", toDelete, e);
    }

    String key = PathBuilder.buildPath(columnShardId.getShardId().shardIdPath(), Constants.LAST_ERROR, transaction.getTarget(), columnShardId.getColumnId().fullName());
    ObjectMetadata omd = new ObjectMetadata();
    omd.setContentLength(size);
    try {
      putObject(key, inputStream, omd, columnShardId.getInterval());
      if (error != null) {
        String description =
          PathBuilder.buildPath(columnShardId.getShardId().shardIdPath(), Constants.LAST_ERROR, transaction.getTarget(), columnShardId.getColumnId().fullName() + "_msg");
        putObject(description, error, columnShardId.getInterval());
      }
    } catch (ResetException e) {
      LOGGER.error("Detected a reset exception, the number of bytes is {}: {}", size, e.getExtraInfo());
      throw e;
    }
    return key;
  }

  @Override
  public void captureWrites(ArmorXact transaction, ShardId shardId, List<Entity> entities, List<WriteRequest> requests, Object deleteEntity) {
    if (transaction == null) {
      LOGGER.warn("Unable to log write requests for id {}: entities={}, writeRequests={}, delete={}", transaction, entities, requests, deleteEntity);
      return;
    }
    // Build out prefix time for order of operations
    LocalDate currentdate = LocalDate.now();
    String datePrefix = currentdate.getYear() + "_" + currentdate.getMonth() + "_" + currentdate.getDayOfMonth() + "_" + System.currentTimeMillis();
    String key = PathBuilder.buildPath(
       shardId.getTenant(),
       Constants.CAPTURE,
       datePrefix,
       shardId.getTable(),
       shardId.getInterval(),
       shardId.getIntervalStart());
    if (shardId.getShardNum() >= 0) {
      key = PathBuilder.buildPath(key, Integer.toString(shardId.getShardNum()));
    }
    try {
      if (entities != null) {
        String payloadName = PathBuilder.buildPath(key, "entities");
        String payload = OBJECT_MAPPER.writeValueAsString(entities);
        putObject(payloadName, payload, shardId.getInterval());
      }
      if (requests != null) {
        String payloadName = PathBuilder.buildPath(key, "writeRequests");
        String payload = OBJECT_MAPPER.writeValueAsString(requests);
        putObject(payloadName, payload, shardId.getInterval());
      }
      if (deleteEntity != null) {
        String payloadName = PathBuilder.buildPath(key, deleteEntity.toString());
        putObject(payloadName, "deleted", shardId.getInterval());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String rootDirectory() {
    return bucket;
  }

  private ShardId toShardId(String tenant, String table, Interval interval, Instant timestamp, String rawShard) {
    String shardName = Paths.get(rawShard).getFileName().toString();
    int shardNum = Integer.parseInt(shardName.replace("shard-", ""));
    return ShardId.buildShardId(tenant, table, interval, timestamp, shardNum);
  }

  @Override
  public void deleteTenant(String tenant) {
    try {
      String toDelete = tenant + Constants.STORE_DELIMETER;
      ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
          .withBucketName(bucket)
          .withPrefix(toDelete);
      ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
      while (true) {
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
          try {
            s3Client.deleteObject(bucket, objectSummary.getKey());
          } catch (AmazonS3Exception e) {
            // Not found could mean another process has already removed it.
            if (e.getStatusCode() != 404) {
              throw e;
            }
          }
        }
        if (objectListing.isTruncated()) {
          objectListing = s3Client.listNextBatchOfObjects(objectListing);
        } else {
          break;
        }
      }
      columnCache.invalidate(tenant);
    } catch (Exception e) {
      LOGGER.warn("Unable to completely remove tenant {}", tenant, e);
      throw e;
    }
  }

  @Override
  public ColumnMetadata getColumnMetadata(String tenant, String table, ColumnShardId columnShardId) {
    String shardIdPath = PathBuilder.buildPath(resolveCurrentPath(columnShardId.getShardId()), columnShardId.getColumnId().fullName());
    try {
      if (!s3Client.doesObjectExist(bucket, shardIdPath)) {
        return null;
      } else {
        try (S3Object s3Object = s3Client.getObject(bucket, shardIdPath); S3ObjectInputStream s3ObjectInputSTream = s3Object.getObjectContent()) {
          try {
            ColumnFileReader reader = new ColumnFileReader();
            reader.read(new DataInputStream(s3Object.getObjectContent()), null);
            return reader.getColumnMetadata();
          } finally {
            com.amazonaws.util.IOUtils.drainInputStream(s3ObjectInputSTream);
          }
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
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
  
  private String resolveCurrentPath(String tenant, String table) {
    DistXactRecord status = getCurrentValues(tenant, table);
    if (status == null || status.getCurrent() == null)
      return null;
    return PathBuilder.buildPath(tenant, table, status.getCurrent());
  }

  private String resolveCurrentPath(ShardId shardId) {
    DistXactRecord status = getCurrentValues(shardId);
    if (status == null || status.getCurrent() == null)
      return null;
    return PathBuilder.buildPath(shardId.shardIdPath(), status.getCurrent());
  }
  
  private String getCurrentShardKey(Path shardSrcPath, List<S3ObjectSummary> currentShardObjects) {
    // armor-bucket/org1/tag/weekly/2021-04-12T00:00:00Z/15/
    // |___________________shardSrcPath____________________|
    
    // currentShardObjectKey =
    // armor-bucket/org1/tag/weekly/2021-04-12T00:00:00Z/15/4419aee7-4c45-433e-a2bd-5a71c1b3ec1b/.../.../.../
    //                                                     |_shardSrcPath.relativize(currentShardObjectKey)_|
    //                                                     |_____________getName(0)_____________|___________|
    //                                                     |__________currentShardName__________|
    // currentShardKey = shardSrcPath + currentShardName
    // currentShardKey = armor-bucket/org1/tag/weekly/2021-04-12T00:00:00Z/15/4419aee7-4c45-433e-a2bd-5a71c1b3ec1b/
    
    if (!currentShardObjects.isEmpty()) {
      Path currentShardObjectKey = Paths.get(currentShardObjects.get(0).getKey());
      Path currentShardName = shardSrcPath.relativize(currentShardObjectKey).getName(0);
      return shardSrcPath.resolve(currentShardName).toString();
    }
    return null;
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
  
  private void saveCurrentValues(String tenant, String table, DistXactRecord status) {
    String key = DistXactRecordUtil.buildCurrentMarker(PathBuilder.buildPath(tenant, table));
    try {
      String payload = DistXactRecordUtil.prepareToCommit(status);
      ObjectMetadata objectMetadata = new ObjectMetadata();
      objectMetadata.setContentType("text/plain");
      objectMetadata.setContentLength(payload.length());
      PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, new StringInputStream(payload), objectMetadata);
      s3Client.putObject(putObjectRequest);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private void saveCurrentValues(ShardId shardId, DistXactRecord status) {
    String key = DistXactRecordUtil.buildCurrentMarker(shardId.shardIdPath());
    try {
      String payload = DistXactRecordUtil.prepareToCommit(status);
      ObjectMetadata objectMetadata = new ObjectMetadata();
      objectMetadata.setContentType("text/plain");
      objectMetadata.setContentLength(payload.length());
      PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, new StringInputStream(payload), objectMetadata);
      putObjectRequest.withTagging(createObjectTagging(shardId.getInterval()));
      s3Client.putObject(putObjectRequest);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private ObjectTagging createObjectTagging(String interval) {
    Tag tag = new Tag(INTERVAL_TAG, interval);
    return new ObjectTagging(Stream.of(tag).collect(toList()));
  }

  private void putObject(String key, String payload, String interval) {
    byte[] contentBytes = payload.getBytes(StringUtils.UTF8);
    InputStream is = new ByteArrayInputStream(contentBytes);
    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentType("text/plain");
    objectMetadata.setContentLength(contentBytes.length);

    putObject(key, is, objectMetadata, interval);
  }

  private void putObject(String key, InputStream payload, ObjectMetadata objectMetadata, String interval) {
    PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, payload, objectMetadata).withTagging(createObjectTagging(interval));
    putObjectRequest.getRequestClientOptions().setReadLimit((int) objectMetadata.getContentLength());
    s3Client.putObject(putObjectRequest);
  }
  
  /**
   * Attempts exists check, if its errors out it is most likely a slowdown error. So sleep for a second and retry again.
   */
  private boolean doesObjectExist(String bucket, String key) {
    for (int i = 0; i < 10; i++) {
      try {
        return s3Client.doesObjectExist(bucket, key);
      } catch (SdkClientException e) {
        if (i == 10) {
          LOGGER.error("Unable to execute existance check on {}:{}..quitting", bucket, key, e);
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
  
  private boolean columnExistsInCache(String tenant, String column) {
    Set<String> columns = columnCache.getIfPresent(tenant);
    if (columns != null) {
      return columns.contains(column);
    }
    return false;
  }
  
  private void trackTenant(String tenant) {
    if (!tenantCache.contains(tenant)) {
      String key = PathBuilder.buildPath(StoreConstants.TENANT_CACHE_DIR, tenant);
      InputStream inputStream = new ByteArrayInputStream(new byte[0]);
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setContentLength(0L);
      
      s3Client.putObject(new PutObjectRequest(bucket, key, inputStream, metadata));
      tenantCache.add(tenant);
    }
  }

  private List<S3ObjectSummary> getCurrentShardObjects(ShardId shardId) {
    String currentShardKey = null;
    for (int i = 0; i < 10; i++) {
      DistXactRecord currentValues = getCurrentValues(shardId);
      if (currentValues != null) {
        List<S3ObjectSummary> objects = new ArrayList<>();
        try {
          currentShardKey = PathBuilder.buildPath(shardId.shardIdPath(), currentValues.getCurrent());
          ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(bucket).withPrefix(currentShardKey);
          ListObjectsV2Result result;
          do {
            result = s3Client.listObjectsV2(request);
            objects.addAll(result.getObjectSummaries());
            request.setContinuationToken(result.getNextContinuationToken());
          } while (result.isTruncated());
          return objects;
        } catch (Exception e) {
          LOGGER.warn("Error retrieving current shard objects for shard: " +  currentShardKey, e);
        }
      }
    }
    return Collections.emptyList();
  }

  @Override
  public void deleteTable(String tenant, String table) {
    try {
      String toDelete = PathBuilder.buildPath(tenant, table) + Constants.STORE_DELIMETER;
      ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
          .withBucketName(bucket)
          .withPrefix(toDelete);
      ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
      while (true) {
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
          s3Client.deleteObject(bucket, objectSummary.getKey());
        }
        if (objectListing.isTruncated()) {
          objectListing = s3Client.listNextBatchOfObjects(objectListing);
        } else {
          break;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Unable completely remove tenant {}", tenant, e);
      throw e;
    }    
    
  }

  @Override
  public void deleteInterval(String tenant, String table, Interval interval) {
    try {
      String toDelete = PathBuilder.buildPath(tenant, table, interval.getInterval()) + Constants.STORE_DELIMETER;
      ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
          .withBucketName(bucket)
          .withPrefix(toDelete);
      ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
      while (true) {
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
          s3Client.deleteObject(bucket, objectSummary.getKey());
        }
        if (objectListing.isTruncated()) {
          objectListing = s3Client.listNextBatchOfObjects(objectListing);
        } else {
          break;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Unable completely remove tenant {}", tenant, e);
      throw e;
    }
  }

  @Override
  public void deleteIntervalStart(String tenant, String table, Interval interval, String intervalStart) {
    try {
      String toDelete = PathBuilder.buildPath(tenant, table, interval.getInterval(), intervalStart) + Constants.STORE_DELIMETER;
      ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
          .withBucketName(bucket)
          .withPrefix(toDelete);
      ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
      while (true) {
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
          s3Client.deleteObject(bucket, objectSummary.getKey());
        }
        if (objectListing.isTruncated()) {
          objectListing = s3Client.listNextBatchOfObjects(objectListing);
        } else {
          break;
        }
      }
    } catch (Exception e) {
      LOGGER.warn("Unable completely remove tenant {}", tenant, e);
      throw e;
    }
  }

  @Override
  public boolean intervalExists(String tenant, String table, Interval interval) {
    String intervalPath = PathBuilder.buildPath(tenant, table, interval.getInterval()) + Constants.STORE_DELIMETER;
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
        .withBucketName(bucket)
        .withPrefix(intervalPath);
    ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
    return !objectListing.getObjectSummaries().isEmpty();
  }

  @Override
  public boolean tableExists(String tenant, String table) {
    String tablePath = PathBuilder.buildPath(tenant, table) + Constants.STORE_DELIMETER;
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
        .withBucketName(bucket)
        .withPrefix(tablePath);
    ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
    return !objectListing.getObjectSummaries().isEmpty();
  }

  @Override
  public boolean intervalStartExists(String tenant, String table, Interval interval, String intervalStart) {
    String intervalPath = PathBuilder.buildPath(tenant, table, interval.getInterval(), intervalStart) + Constants.STORE_DELIMETER;
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
        .withBucketName(bucket)
        .withPrefix(intervalPath);
    ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
    return !objectListing.getObjectSummaries().isEmpty();
  }

  @Override
  public boolean columnShardIdExists(ColumnShardId columnShardId) {
    String shardIdPath = PathBuilder.buildPath(resolveCurrentPath(columnShardId.getShardId()), columnShardId.getColumnId().fullName());
    return s3Client.doesObjectExist(bucket, shardIdPath);
  }

  @Override
  public ColumnId getEntityIdColumn(String tenant, String table) {
    //Check cache first
    Set<String> cachedColumns = columnCache.getIfPresent(tenant);
    if (cachedColumns != null) {
      String cachedEntityColumn = cachedColumns
          .stream()
          .filter(column -> column.startsWith(ColumnId.ENTITY_COLUMN_IDENTIFIER))
          .findFirst()
          .orElse(null);
      if (cachedEntityColumn != null) {
        return new ColumnId(cachedEntityColumn.substring(1));
      }
    }
    
    //Get from s3 if not in cache
    String entityColumnPath = PathBuilder.buildPath(tenant, table, COLUMN_METADATA_DIR, ColumnId.ENTITY_COLUMN_IDENTIFIER);
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
        .withBucketName(bucket)
        .withPrefix(entityColumnPath);
    
    ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
    if (!objectListing.getObjectSummaries().isEmpty()) {
      String columnFullName = Paths.get(objectListing.getObjectSummaries().get(0).getKey()).getFileName().toString().substring(1);
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
