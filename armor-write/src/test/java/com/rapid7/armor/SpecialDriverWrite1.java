package com.rapid7.armor;


import com.rapid7.armor.entity.Entity;
import com.rapid7.armor.schema.ColumnName;
import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ModShardStrategy;
import com.rapid7.armor.store.FileWriteStore;
import com.rapid7.armor.write.ArmorWriter;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpecialDriverWrite1 {
  private static final Logger LOGGER = LoggerFactory.getLogger(SpecialDriverWrite1.class);
  private static final long K = 1024;
  private static final long M = K * K;
  private static final long G = M * K;
  private static final long T = G * K;

  public static String convertToStringRepresentation(final long value) {
    final long[] dividers = new long[] {T, G, M, K, 1};
    final String[] units = new String[] {"TB", "GB", "MB", "KB", "B"};
    if (value < 1)
      throw new IllegalArgumentException("Invalid file size: " + value);
    String result = null;
    for (int i = 0; i < dividers.length; i++) {
      final long divider = dividers[i];
      if (value >= divider) {
        result = format(value, divider, units[i]);
        break;
      }
    }
    return result;
  }

  private static String format(final long value,
                               final long divider,
                               final String unit) {
    final double result =
        divider > 1 ? (double) value / (double) divider : (double) value;
    return new DecimalFormat("#,##0.#").format(result) + " " + unit;
  }

  private static List<ColumnName> buildColumns() {
    return Arrays.asList(
        new ColumnName("family", DataType.INTEGER.getCode()),
        new ColumnName("vendor", DataType.INTEGER.getCode()),
        new ColumnName("product", DataType.INTEGER.getCode()),
        new ColumnName("version", DataType.INTEGER.getCode()));
  }

  public static void main(String[] args) throws IOException {
    ModShardStrategy mss = new ModShardStrategy(10);
    FileWriteStore fileStore = new FileWriteStore(Paths.get("/home/alee/nike/armor"), mss);
    ArmorWriter aw = new ArmorWriter("test", fileStore, 10, false, null, null);
    String transction = aw.startTransaction();
    Instant start = Instant.now();
    try (BufferedReader csvReader = new BufferedReader(new FileReader("/home/alee/nike/nike-sort1.csv"))) {
      String row;

      boolean headersSet = false;
      int count = 0;
      int assetIdColumn = -1;
      int familyColumn = -1;
      int productColumn = -1;
      int vendorColumn = -1;
      int versionColumn = -1;
      Asset currentAsset = new Asset();
      List<Entity> entities = new ArrayList<>();
      while ((row = csvReader.readLine()) != null) {
        count++;
        if (count % 10000000 == 0) {
          System.out.println("Traversed through " + count + " going to write for " + entities.size() + " entities");
          long mark = System.currentTimeMillis();
          aw.write(transction, "testorg1", "asset_sw_int", entities);
          System.out.println("End write took " + (System.currentTimeMillis() - mark) / 1000 + " seconds");
          long mark2 = System.currentTimeMillis();
          aw.save(transction, "testorg1", "asset_sw_int");
          entities.clear();
          System.out.println("End saving took " + (System.currentTimeMillis() - mark2) / 1000 + " seconds");
        }
        String[] data = row.split(",", -1);
        if (!headersSet) {
          boolean isHeader = Arrays.stream(data).anyMatch(d -> d.equalsIgnoreCase("assetId"));
          if (!isHeader) {
            System.out.println("Need headers " + row);
            throw new RuntimeException("Need headers, here are the rows " + data);
          } else
            System.out.println("Detected headers " + row);

          for (int ii = 0; ii < data.length; ii++) {
            if (data[ii].equalsIgnoreCase("assetId"))
              assetIdColumn = ii;
            else if (data[ii].equalsIgnoreCase("family"))
              familyColumn = ii;
            else if (data[ii].equalsIgnoreCase("product"))
              productColumn = ii;
            else if (data[ii].equalsIgnoreCase("version"))
              versionColumn = ii;
            else if (data[ii].equalsIgnoreCase("vendor"))
              vendorColumn = ii;
          }
          LOGGER.info("The columns selected are {}:{}:{}:{}:{}", assetIdColumn, familyColumn, productColumn, versionColumn, vendorColumn);
          headersSet = true;
          continue;
        }
        try {
          String assetId = data[assetIdColumn];
          if (currentAsset == null || currentAsset.assetId == null || !currentAsset.assetId.equals(assetId)) {
            if (currentAsset != null && currentAsset.assetId != null) {
              Entity entity = Entity.buildEntity("assetId", currentAsset.assetId, System.currentTimeMillis(), null, buildColumns());
              for (Software s : currentAsset.software) {
                entity.addRow(s.family, s.vendor, s.product, s.version);
              }
              entities.add(entity);
            }
            currentAsset = new Asset();
            currentAsset.assetId = assetId;
          }

          Software sw = new Software();
          if (data.length > familyColumn)
            sw.family = data[familyColumn].hashCode();
          if (data.length > productColumn)
            sw.product = data[productColumn].hashCode();
          if (data.length > vendorColumn)
            sw.vendor = data[vendorColumn].hashCode();
          if (data.length > versionColumn)
            sw.version = data[versionColumn].hashCode();

          currentAsset.software.add(sw);
        } catch (ArrayIndexOutOfBoundsException e) {
          LOGGER.info("Unexpected error for {}", row, e);
          throw e;
        }
      }
      aw.close();
      System.out.println("going to exit took " + Duration.between(start, Instant.now()));
      System.exit(0);
    }
  }

  public static class Asset {
    private String assetId;
    private final List<Software> software = new ArrayList<>();
  }

  public static class Software {
    private int family;
    private int vendor;
    private int product;
    private int version;
  }
}
