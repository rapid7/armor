package com.rapid7.armor.write.component;

import java.io.IOException;
import java.io.InputStream;

public interface ExtendedIndexWriter extends Component, IndexUpdater {
   /**
    * an integer extended type which will be used for serialization and dispatch in ColumnFileWriter.
    * The ExtendedIndex section will include a section header that indicates which type it is.
    * Thus, extendedType must be unique.
    *
    * @return the extendedType value.
    */
   long extendedType();

   void load(InputStream data) throws IOException;
}
