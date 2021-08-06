package com.rapid7.armor.write.component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;

public class ComponentHeaderWriter implements Component
{
   private final Component component;
   private final byte[] header;

   public ComponentHeaderWriter(byte[] header, Component c)
   {
      this.component = c;
      this.header = header;
   }

   @Override public InputStream getInputStream()
      throws IOException
   {
      return new SequenceInputStream(new ByteArrayInputStream(this.header), component.getInputStream());
   }

   @Override public long getCurrentSize()
      throws IOException
   {
      return this.header.length + component.getCurrentSize();
   }
}
