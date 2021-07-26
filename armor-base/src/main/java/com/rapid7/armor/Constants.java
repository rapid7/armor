package com.rapid7.armor;

public final class Constants {
  public final static String CAPTURE = "capture";
  public final static String LAST_ERROR = "lasterror";
  public final static String STORE_DELIMETER = "/";
  public final static int NULL_FILLER_INDICATOR = -123;
  // Entity record constants
  public final static int ID_BYTE_LENGTH = 4;
  public final static int ROWGROUP_OFFSET_BYTE_LENGTH = 4;
  public final static int VALUE_LENGTH_BYTE_LENGTH = 4;
  public final static int VERSION_BYTE_LENGTH = 8;
  public final static int DELETED_BYTE_LENGTH = 1;
  public final static int NULL_BYTE_LENGTH = 4;
  public final static int DECODED_BYTE_LENGTH = 4;
  public final static int INSTANCE_ID_BYTE_LENGTH = 36;

  public final static int RECORD_SIZE_BYTES =
      ID_BYTE_LENGTH +
      ROWGROUP_OFFSET_BYTE_LENGTH +
      VALUE_LENGTH_BYTE_LENGTH +
      VERSION_BYTE_LENGTH +
      DELETED_BYTE_LENGTH +
      NULL_BYTE_LENGTH +
      DECODED_BYTE_LENGTH +
      INSTANCE_ID_BYTE_LENGTH;

  public final static int BEGIN_OFFSET_OFFSET = ID_BYTE_LENGTH;
  public final static int BEGIN_VALUE_LENGTH_OFFSET = ID_BYTE_LENGTH + ROWGROUP_OFFSET_BYTE_LENGTH;
  public final static int BEGIN_DELETE_OFFSET = ID_BYTE_LENGTH + ROWGROUP_OFFSET_BYTE_LENGTH + VALUE_LENGTH_BYTE_LENGTH + VERSION_BYTE_LENGTH;
  public final static int BEGIN_NULL_BYTE_LENGTH_OFFSET = BEGIN_DELETE_OFFSET + DELETED_BYTE_LENGTH;
  public final static int BEGIN_REAL_LENGTH_OFFSET = BEGIN_DELETE_OFFSET + DELETED_BYTE_LENGTH + NULL_BYTE_LENGTH;
  public final static int BEGIN_INSTANCE_ID_LENGTH_OFFSET = BEGIN_DELETE_OFFSET + DELETED_BYTE_LENGTH + NULL_BYTE_LENGTH + DECODED_BYTE_LENGTH;


  public static enum ColumnFileFormatVersion {
    VERSION_1(0x0001),
    VERSION_2(0x0002);

    private final int val;

    ColumnFileFormatVersion(int i)
    {
      this.val = i;
    }
    public int getVal() {
      return val;
    }
  };

  // Default version that is written, for backward compatible APIs.
  public final static ColumnFileFormatVersion DEFAULT_VERSION = ColumnFileFormatVersion.VERSION_2;
  
  // Version that is supported. If there is a change to the format, then update this value.
  public final static int VERSION = 0x0001;

  // Designates start of armor column
  public final static short MAGIC_HEADER = (short) 0xA1EE;

  public final static String COLUMN_METADATA_DIR = "metadata";
  public final static String SHARD_METADATA = "shard-metadata";

}
