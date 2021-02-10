package com.rapid7.armor.write.component;

import java.io.IOException;
import java.io.InputStream;

/**
 * Component writers are writers that write to one of the 5 components of a column shard, they are
 * a) Dictionary
 * b) RowGroup
 * c) Entity
 * d) NullableBitMap
 * e) Metadata
 * <p>
 * All of these writers are store their values in memory in order to quickly record changes, when the writes
 * are deemed ready to presist, it must provide an input stream for the managing writer to package and eventully
 * send down to the datastore layer.
 */
public interface Component {

  InputStream getInputStream() throws IOException;

  long getCurrentSize() throws IOException;
}
