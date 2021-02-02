package com.rapid7.armor;

public final class Constants {
  public final static String CAPTURE = "capture";
  public final static String CURRENT = "CURRENT";
  public final static String LAST_ERROR = "lasterror";

  public final static int NULL_FILLER_INDICATOR = -123;
  // Entity record constants
  public final static int ID_BYTE_LENGTH = 4;
  public final static int ROWGROUP_OFFSET_BYTE_LENGTH = 4;
  public final static int VALUE_LENGTH_BYTE_LENGTH = 4;
  public final static int VERSION_BYTE_LENGTH = 8;
  public final static int DELETED_BYTE_LENGTH = 1;
  public final static int NULL_BYTE_LENGTH = 4;
  public final static int REAL_BYTE_LENGTH = 4;
  public final static int INSTANCE_ID_BYTE_LENGTH = 36;

  public final static int RECORD_SIZE_BYTES =
      ID_BYTE_LENGTH +
          ROWGROUP_OFFSET_BYTE_LENGTH +
          VALUE_LENGTH_BYTE_LENGTH +
          VERSION_BYTE_LENGTH +
          DELETED_BYTE_LENGTH +
          NULL_BYTE_LENGTH +
          REAL_BYTE_LENGTH +
          INSTANCE_ID_BYTE_LENGTH;

  public final static int BEGIN_OFFSET_OFFSET = ID_BYTE_LENGTH;
  public final static int BEGIN_VALUE_LENGTH_OFFSET = ID_BYTE_LENGTH + ROWGROUP_OFFSET_BYTE_LENGTH;
  public final static int BEGIN_DELETE_OFFSET = ID_BYTE_LENGTH + ROWGROUP_OFFSET_BYTE_LENGTH + VALUE_LENGTH_BYTE_LENGTH + VERSION_BYTE_LENGTH;
  public final static int BEGIN_NULL_BYTE_LENGTH_OFFSET = BEGIN_DELETE_OFFSET + DELETED_BYTE_LENGTH;
  public final static int BEGIN_REAL_LENGTH_OFFSET = BEGIN_DELETE_OFFSET + DELETED_BYTE_LENGTH + NULL_BYTE_LENGTH;
  public final static int BEGIN_INSTANCE_ID_LENGTH_OFFSET = BEGIN_DELETE_OFFSET + DELETED_BYTE_LENGTH + NULL_BYTE_LENGTH + REAL_BYTE_LENGTH;


  // Designates start of armor column
  public final static short MAGIC_HEADER = (short) 0xA1EE;
  // Designates start of armor entity column
  public final static short MAGIC_ENTITY_HEADER = (short) 0xA7EE;

  public final static String TABLE_METADATA = "table-metadata";
  public final static String SHARD_METADATA = "shard-metadata";

  // Denotes end, useful to smash columns together into 1 file.
  //public final static short MAGIC_FOOTER = (short) 0xA7EE;

}
