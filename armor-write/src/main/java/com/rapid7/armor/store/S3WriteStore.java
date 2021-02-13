package com.rapid7.armor.store;

import com.rapid7.armor.Constants;
import com.rapid7.armor.entity.Entity;
import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.meta.TableMetadata;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.shard.ShardStrategy;
import com.rapid7.armor.write.WriteRequest;
import com.rapid7.armor.write.writers.ColumnFileWriter;
import com.amazonaws.ResetException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.rapid7.armor.Constants.INTERVAL_UNITS;

public class S3WriteStore implements WriteStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3WriteStore.class);
  private final AmazonS3 s3Client;
  private final String bucket;
  private final ShardStrategy shardStrategy;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public S3WriteStore(AmazonS3 s3Client, String bucket, ShardStrategy shardStrategy) {
    this.s3Client = s3Client;
    this.bucket = bucket;
    this.shardStrategy = shardStrategy;
  }

  @Override
  public ShardId buildShardId(String tenant, String table, long interval, Instant timestamp, int shardNum) {
    return new ShardId(tenant, table, interval, timestamp, shardNum);
  }

  @Override
  public ShardId findShardId(String tenant, String table, long interval, Instant timestamp, Object entityId) {
    int shardNum = shardStrategy.shardNum(entityId);
    return buildShardId(tenant, table, interval, timestamp, shardNum);
  }

  @Override
  public void saveColumn(String transactionId, ColumnShardId columnShardId, int byteSize, InputStream inputStream) {
    String key = getIntervalPrefix(columnShardId.getShardId()) + "/" + columnShardId.getShardNum() + "/" + transactionId + "/" +
        columnShardId.getColumnId().fullName();
    ObjectMetadata omd = new ObjectMetadata();
    omd.setContentLength(byteSize);
    try {
      PutObjectRequest por = new PutObjectRequest(bucket, key, inputStream, omd);
      por.getRequestClientOptions().setReadLimit(byteSize);
      s3Client.putObject(por);
    } catch (ResetException e) {
      LOGGER.error("Detected a reset exception, the number of bytes is {}: {}", byteSize, e.getExtraInfo());
      throw e;
    }
  }

  @Override
  public ColumnFileWriter loadColumnWriter(ColumnShardId columnShardId) {
    String shardIdPath = resolveCurrentPath(columnShardId.getTenant(), columnShardId.getTable(), columnShardId.getInterval(), columnShardId.getIntervalStart(), columnShardId.getShardNum()) + "/" + columnShardId.getColumnId().fullName();
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
    lor.withDelimiter("/");
    lor.withPrefix(resolveCurrentPath(shardId.getTenant(), shardId.getTable(), shardId.getInterval(), shardId.getIntervalStart(), shardId.getShardNum()) + "/");
    ListObjectsV2Result ol = s3Client.listObjectsV2(lor);
    List<S3ObjectSummary> summaries = ol.getObjectSummaries();
    return summaries.stream()
        .map(s -> Paths.get(s.getKey()).getFileName().toString())
        .filter(n -> !n.contains(Constants.SHARD_METADATA))
        .map(ColumnId::new).collect(Collectors.toList());
  }

  @Override
  public List<ShardId> findShardIds(String tenant, String table, long interval, Instant timestamp) {
    ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucket).withMaxKeys(10000);
    lor.withDelimiter("/");
    lor.withPrefix(getIntervalPrefix(tenant, table, interval, timestamp) + "/");
    ListObjectsV2Result ol = s3Client.listObjectsV2(lor);
    List<String> commonPrefixes = ol.getCommonPrefixes();
    // Remove trailing /
    List<String> rawShardNames = commonPrefixes.stream().map(cp -> cp.substring(0, cp.length() - 1)).collect(Collectors.toList());
    return rawShardNames.stream().map(s -> toShardId(tenant, table, interval, timestamp, s)).collect(Collectors.toList());
  }

  @Override
  public List<ShardId> findShardIds(String tenant, String table, long interval, Instant timestamp, String columnId) {
    ListObjectsV2Request lor = new ListObjectsV2Request().withBucketName(bucket).withMaxKeys(10000);
    lor.withDelimiter("/");
    lor.withPrefix(getIntervalPrefix(tenant, table, interval, timestamp) + "/");
    ListObjectsV2Result ol = s3Client.listObjectsV2(lor);
    List<String> commonPrefixes = ol.getCommonPrefixes();
    // Remove trailing /
    List<String> rawShardNames = commonPrefixes.stream().map(cp -> cp.substring(0, cp.length() - 1)).collect(Collectors.toList());
    return rawShardNames.stream().map(s -> toShardId(tenant, table, interval, timestamp, s)).collect(Collectors.toList());
  }

  @Override
  public int findShardNum(Object entityId) {
    return shardStrategy.shardNum(entityId);
  }

  @Override
  public List<ColumnFileWriter> loadColumnWriters(String tenant, String table, long interval, Instant timestamp, int shardNum) {
    ShardId shardId = buildShardId(tenant, table, interval, timestamp, shardNum);
    List<ColumnId> columnIds = getColumnIds(buildShardId(tenant, table, interval, timestamp, shardNum));
    List<ColumnFileWriter> writers = new ArrayList<>();
    for (ColumnId columnId : columnIds) {
      String shardIdPath = resolveCurrentPath(tenant, table, interval, timestamp, shardId.getShardNum()) + "/" + columnId.fullName();
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
  public TableMetadata loadTableMetadata(String tenant, String table) {
    String relativeTarget = tenant + "/" + table + "/" + Constants.TABLE_METADATA + ".armor";
    try {
      if (doesObjectExist(bucket, relativeTarget)) {
        try (S3Object s3Object = s3Client.getObject(bucket, relativeTarget); S3ObjectInputStream s3InputStream = s3Object.getObjectContent()) {
          try {
            return OBJECT_MAPPER.readValue(s3InputStream, TableMetadata.class);
          } finally {
            com.amazonaws.util.IOUtils.drainInputStream(s3InputStream);
          }
        } catch (IOException jpe) {
          throw new RuntimeException(jpe);
        }
      } else
        return null;
    } catch (AmazonS3Exception as3) {
      LOGGER.error("Unable to load metadata at on {} at {}", bucket, relativeTarget);
      throw as3;
    }
  }

  @Override
  public void saveTableMetadata(String transaction, String tenant, String table, TableMetadata tableMetadata) {
    String relativeTarget = tenant + "/" + table + "/" + Constants.TABLE_METADATA + ".armor";
    try {
      String payload = OBJECT_MAPPER.writeValueAsString(tableMetadata);
      s3Client.putObject(bucket, relativeTarget, payload);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public ShardMetadata loadShardMetadata(String tenant, String table, long interval, Instant timestamp, int shardNum) {
    String shardIdPath = resolveCurrentPath(tenant, table, interval, timestamp, shardNum) + "/" + Constants.SHARD_METADATA + ".armor";

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
  public void saveShardMetadata(String transactionId, String tenant, String table, long interval, Instant timestamp, int shardNum, ShardMetadata shardMetadata) {
    ShardId shardId = buildShardId(tenant, table, interval, timestamp, shardNum);
    String shardIdPath = shardId.getShardId() + "/" + transactionId + "/" + Constants.SHARD_METADATA + ".armor";
    for (int i = 0; i < 10; i++) {
      try {
        String payload = OBJECT_MAPPER.writeValueAsString(shardMetadata);
        s3Client.putObject(bucket, shardIdPath, payload);
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
  public void commit(String transaction, String tenant, String table, long interval, Instant timestamp, int shardNum) {
    Map<String, String> currentValues = getCurrentValues(tenant, table, interval, timestamp, shardNum);
    String oldCurrent = null;
    String oldPrevious = null;
    if (currentValues != null) {
      oldCurrent = currentValues.get("current");
      oldPrevious = currentValues.get("previous");
    }
    if (oldCurrent != null && oldCurrent.equalsIgnoreCase(transaction))
      throw new RuntimeException("Create another transaction");
    saveCurrentValues(tenant, table, interval, timestamp, shardNum, transaction, oldCurrent);
    try {
      String toDelete = getIntervalPrefix(tenant, table, interval, timestamp) + "/" + shardNum + "/" + oldPrevious;
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
      LOGGER.warn("Unable to previous shard version under {}", oldPrevious, e);
    }
  }

  @Override
  public String resolveCurrentPath(String tenant, String table, long interval, Instant timestamp, int shardNum) {
    Map<String, String> values = getCurrentValues(tenant, table, interval, timestamp, shardNum);
    String current = values.get("current");
    if (current == null)
      return null;
    return getIntervalPrefix(tenant, table, interval, timestamp) + "/" + shardNum + "/" + current;
  }

  @Override
  public Map<String, String> getCurrentValues(String tenant, String table, long interval, Instant timestamp, int shardNum) {
    String key = getIntervalPrefix(tenant, table, interval, timestamp) + "/" + shardNum + "/" + Constants.CURRENT;
    if (!doesObjectExist(this.bucket, key))
      return new HashMap<>();
    else {
      try (S3Object s3Object = s3Client.getObject(bucket, key); S3ObjectInputStream inputStream = s3Object.getObjectContent()) {
        return OBJECT_MAPPER.readValue(inputStream, new TypeReference<Map<String, String>>() {});
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  @Override
  public void saveCurrentValues(String tenant, String table, long interval, Instant timestamp, int shardNum, String current, String previous) {
    String key = getIntervalPrefix(tenant, table, interval, timestamp) + "/" + shardNum + "/" + Constants.CURRENT;
    try {
      HashMap<String, String> currentValues = new HashMap<>();
      currentValues.put("current", current);
      if (previous != null)
        currentValues.put("previous", previous);
      String payload = OBJECT_MAPPER.writeValueAsString(currentValues);
      s3Client.putObject(bucket, key, payload);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
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

  @Override
  public void rollback(String transaction, String tenant, String table, long interval, Instant timestamp, int shardNum) {
    String toDelete = getIntervalPrefix(tenant, table, interval, timestamp) + "/" + shardNum + "/" + transaction;
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
  public void saveError(String transaction, ColumnShardId columnShardId, int size, InputStream inputStream, String error) {
    // First erase any previous errors that may have existed before.
    String toDelete = getIntervalPrefix(columnShardId.getShardId()) + "/" + columnShardId.getShardNum() + "/" + Constants.LAST_ERROR;
    try {
      ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
          .withBucketName(bucket)
          .withPrefix(toDelete);
      ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
      while (true) {
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
          if (!objectSummary.getKey().contains(transaction))
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

    String key = columnShardId.getTenant() + "/" + columnShardId.getTable() + "/" + columnShardId.getShardNum() + "/" + Constants.LAST_ERROR + "/" + transaction + "/" + columnShardId.getColumnId().fullName();
    ObjectMetadata omd = new ObjectMetadata();
    omd.setContentLength(size);
    try {
      PutObjectRequest por = new PutObjectRequest(bucket, key, inputStream, omd);
      por.getRequestClientOptions().setReadLimit(size);
      s3Client.putObject(por);
      if (error != null) {
        String description =
          columnShardId.getTenant() + "/" +
          columnShardId.getTable() + "/" +
          columnShardId.getShardNum() + "/" +
          Constants.LAST_ERROR + "/" +
          transaction + "/" + columnShardId.getColumnId().fullName() + "_msg";
        s3Client.putObject(bucket, description, error);
      }
    } catch (ResetException e) {
      LOGGER.error("Detected a reset exception, the number of bytes is {}: {}", size, e.getExtraInfo());
      throw e;
    }
  }

  @Override
  public void captureWrites(String transaction, ShardId shardId, List<Entity> entities, List<WriteRequest> requests, Object deleteEntity) {
    if (transaction == null) {
      LOGGER.warn("Unable to log write requests for id {}: entities={}, writeRequests={}, delete={}", transaction, entities, requests, deleteEntity);
      return;
    }

    String key = shardId.getTenant() + "/" + Constants.CAPTURE + "/" + transaction + "/" + shardId.getTable();
    if (shardId.getShardNum() >= 0) {
      key = key + "/" + shardId.getShardNum();
    }
    try {
      if (entities != null) {
        String payloadName = key + "/" + "entities";
        String payload = OBJECT_MAPPER.writeValueAsString(entities);
        s3Client.putObject(bucket, payloadName, payload);
      }
      if (requests != null) {
        String payloadName = key + "/" + "writeRequests";
        String payload = OBJECT_MAPPER.writeValueAsString(requests);
        s3Client.putObject(bucket, payloadName, payload);
      }
      if (deleteEntity != null) {
        String payloadName = key + "/" + deleteEntity;
        s3Client.putObject(bucket, payloadName, "deleted");
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String rootDirectory() {
    return bucket;
  }

  private ShardId toShardId(String tenant, String table, long interval, Instant timestamp, String rawShard) {
    String shardName = Paths.get(rawShard).getFileName().toString();
    int shardNum = Integer.parseInt(shardName.replace("shard-", ""));
    return buildShardId(tenant, table, interval, timestamp, shardNum);
  }

  private String getIntervalPrefix(ShardId shardId) {
    return shardId.getTenant() + "/" + shardId.getTable() + "/" + shardId.getInterval() + "/" + shardId.getIntervalStart();
  }

  private String getIntervalPrefix(String tenant, String table, long interval, Instant timestamp) {
    return tenant + "/" + table + "/" + interval + "/" + timestampToIntervalStart(interval, timestamp);
  }

  private Instant timestampToIntervalStart(long interval, Instant timestamp) {
    return Instant.ofEpochMilli(timestamp.toEpochMilli() / (interval * INTERVAL_UNITS));
  }
}
