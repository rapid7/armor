package com.rapid7.armor.write.component;

public interface IndexUpdater
{
   void add(Number value, Integer entityId, Integer rowCount);

   void finish();
}
