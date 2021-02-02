package com.rapid7.armor.meta;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.List;

@JsonInclude(value = Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ShardMetadata {

  private List<ColumnMetadata> columnMetadata;

  public List<ColumnMetadata> getColumnMetadata() {
    return columnMetadata;
  }

  public void setColumnMetadata(List<ColumnMetadata> columnMetadata) {
    this.columnMetadata = columnMetadata;
  }

}
