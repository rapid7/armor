package com.rapid7.armor.write.component;

import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ColumnShardId;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class ValueIndexWriter implements ExtendedIndexWriter {
  private final ColumnShardId columnShardId;
  private final DataType dataType;
  private Map<Number, Set<Integer>> valToEntities = new HashMap<>();
  private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  
  public ValueIndexWriter(ColumnShardId columnShardId) {
    this.columnShardId = columnShardId;
    this.dataType = columnShardId.getColumnId().dataType();
  }

  @Override public long extendedType()
  {
    return 0x31460001L; // value must be globally unique
  }

  @Override public void load(InputStream json) throws IOException {
    @SuppressWarnings("unchecked")
    Map<String, List<Integer>> map = OBJECT_MAPPER.readValue(json, Map.class);
    valToEntities = new HashMap<>();
    int highestSurrogate = -1;
    for (Map.Entry<String, List<Integer> > e : map.entrySet()) {
      Number val = dataType.fromString(e.getKey());
      valToEntities.put(val, new HashSet<>(e.getValue()));
    }
  }

  public Map<Number, Set<Integer>> getValToEntities() { return valToEntities; }

  public boolean isEmpty() {
    return valToEntities.isEmpty();
  }

  private byte[] toBytes() throws JsonProcessingException {
    HashMap<String, List<Integer>> serializable = new HashMap<>();
    for (Map.Entry<Number, Set<Integer>> e : valToEntities.entrySet()) {
      serializable.put(this.dataType.toString(e.getKey()), new ArrayList<>(e.getValue()));
    }
    return OBJECT_MAPPER.writeValueAsBytes(serializable);
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return new ByteArrayInputStream(toBytes());
  }

  @Override
  public long getCurrentSize() throws IOException {
    return toBytes().length;
  }



  @Override public void add(Number value, Integer entityId, Integer rowCount)
  {
    Set<Integer> entities = this.valToEntities.get(value);
    if (entities == null) {
      entities = new HashSet<>();
      this.valToEntities.put(value, entities);
    }
    entities.add(entityId);
  }

  @Override public void finish()
  {
    // don't need to do anything
  }
}
