package com.rapid7.armor.write.writers;

import com.rapid7.armor.columnfile.ColumnFileListener;
import com.rapid7.armor.columnfile.ColumnFileReader;
import com.rapid7.armor.columnfile.ColumnFileSection;
import com.rapid7.armor.entity.Column;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.entity.EntityRecordSummary;
import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.io.Compression;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.shard.ModShardStrategy;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.write.StreamProduct;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.*;

import com.rapid7.armor.write.WriteRequest;
import com.rapid7.armor.write.component.ValueIndexWriter;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ColumnFileReaderWriterTest {
   private static List<ColumnId> COLUMNS = Arrays.asList(
      new ColumnId("status", DataType.INTEGER.getCode()),
      new ColumnId("time", DataType.LONG.getCode()),
      new ColumnId("vuln", DataType.STRING.getCode()));


   private static final String TENANT = "test_tenant";
   private static final String TABLE = "test_table";
   private static final Interval INTERVAL = Interval.SINGLE;
   private static final Instant TIMESTAMP = Instant.now();
   private static String TEST_UUID = UUID.randomUUID().toString();
   private static final String ASSET_ID = "assetId";
   private static final Random RANDOM = new Random();

   @Test
   public void testWriteThenRead()
      throws IOException {
      ModShardStrategy shardStrategy = new ModShardStrategy(10);
      int entity1Shard = shardStrategy.shardNum(1);
      ColumnId testColumn = new ColumnId("vuln", DataType.STRING.getCode());

      ColumnShardId columnShardId = new ColumnShardId(new ShardId(TENANT, TABLE, INTERVAL.getInterval(), INTERVAL.getIntervalStart(TIMESTAMP), entity1Shard), testColumn);

      ColumnFileWriter cfw = new ColumnFileWriter(columnShardId);
      StreamProduct result = cfw.buildInputStream(Compression.NONE);
      assertNotNull(result);
      byte[] bytes = bytesFromStreamProduct(result);
      assertEquals(result.getByteSize(), bytes.length);
      assertTrue(bytes.length > 0);
      System.out.println("size: " + bytes.length);

      runColumnFileListener(bytes, printListener());

   }


   @Test
   public void testWriteThenReadV2()
      throws IOException {
      ModShardStrategy shardStrategy = new ModShardStrategy(10);
      int entity1Shard = shardStrategy.shardNum(1);
      ColumnId testColumn = new ColumnId("vuln", DataType.STRING.getCode());

      ColumnShardId columnShardId = new ColumnShardId(new ShardId(TENANT, TABLE, INTERVAL.getInterval(), INTERVAL.getIntervalStart(TIMESTAMP), entity1Shard), testColumn);

      ColumnFileWriter cfw = new ColumnFileWriter(columnShardId);
      StreamProduct result = cfw.buildInputStreamV2(Compression.NONE);
      assertNotNull(result);
      byte[] bytes = bytesFromStreamProduct(result);
      assertEquals(result.getByteSize(), bytes.length);
      assertTrue(bytes.length > 0);

      runColumnFileListener(bytes, printListener());
      return;
   }

   static private class FoundValueIndex {
      boolean foundValueIndex = false;
   };

   @Test
   public void testValueIndex()
      throws IOException {
      ModShardStrategy shardStrategy = new ModShardStrategy(10);
      int entity1Shard = shardStrategy.shardNum(1);
      ColumnId testColumn = new ColumnId("vuln", DataType.LONG);

      ColumnShardId columnShardId = new ColumnShardId(new ShardId(TENANT, TABLE, INTERVAL.getInterval(), INTERVAL.getIntervalStart(TIMESTAMP), entity1Shard), testColumn);

      ColumnFileWriter cfw = new ColumnFileWriter(columnShardId);
      List<WriteRequest> writeRequests = new ArrayList<>();
      for(int i = 0 ; i < 10 ; ++i)
      {
         Number entityId = 1000 + i;
         long version = 1;
         String randomId = UUID.randomUUID().toString();
         Column ecv = new Column(testColumn);
         for (long j = 0 ; j < 10; ++j)
         {
            ecv.addValue(j);
         }

         WriteRequest wr = new WriteRequest(entityId, version, randomId, ecv);
         writeRequests.add(wr);
      }
      cfw.write(writeRequests);

      StreamProduct result = cfw.buildInputStreamV2(Compression.NONE);
      assertNotNull(result);
      byte[] bytes = bytesFromStreamProduct(result);
      assertEquals(result.getByteSize(), bytes.length);
      assertTrue(bytes.length > 0);

      runColumnFileListener(bytes, printListener());
      FoundValueIndex fvi = new FoundValueIndex();
      runColumnFileListener(bytes, new ColumnFileListener()
      {
         @Override public int columnFileSection(ColumnFileSection armorSection, ColumnMetadata metadata, DataInputStream inputStream, int compressedLength, int uncompressedLength)
         {
            if (armorSection == ColumnFileSection.VALUE_INDEX) {
               assertEquals(compressedLength, 0);
               assertEquals(uncompressedLength, 561);
               byte[] json = new byte[561];
               int bytesRead = 0;
               try
               {
                  bytesRead = inputStream.read(json);
               }
               catch (IOException e)
               {
                  assertFalse(true, "error reading inputStream for 561 bytes");
               }

               System.out.println(json);
               try
               {
                  ValueIndexWriter x = new ValueIndexWriter(columnShardId, json);
                  Set<Integer> vals = x.getValToEntities().get(0L);
                  assertEquals(10, vals.size());
               }
               catch (IOException e)
               {
                  assertFalse(true, "error while parsing valueIndexWriter");
               }
               fvi.foundValueIndex = true;
               return bytesRead;
            }
            return 0;
         }
      });
      assertTrue(fvi.foundValueIndex, "found value index");
      return;
   }

   private ColumnFileListener printListener() {
      return new ColumnFileListener() {
         @Override
         public int columnFileSection(
                 ColumnFileSection armorSection, ColumnMetadata metadata, DataInputStream inputStream, int compressedLength, int uncompressedLength) {
            System.out.println("Got section " + armorSection + " compressed: " + compressedLength + " uncompressed: " + uncompressedLength);
            return 0;
         }
      };
   }

   private void runColumnFileListener(byte[] bytes, ColumnFileListener listener) throws IOException {
      DataInputStream str = new DataInputStream(ByteSource.wrap(bytes).openStream());
      ColumnFileReader cfr = new ColumnFileReader();
      cfr.read(str, listener);
   }


   private byte[] bytesFromStreamProduct(StreamProduct result2) throws IOException {
      InputStream is2 = result2.getInputStream();
      return ByteStreams.toByteArray(is2);
   }
}
