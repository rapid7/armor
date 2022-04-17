package com.rapid7.armor.write.component;

import java.util.Collection;

public interface IndexUpdater
{

   void add(Number value, Integer entityId, Integer rowCount);

   void finish();

   /**
    * create an indexUpdater that wraps a collection of updaters.
    * @return wrapping indexUpdater
    */
   static IndexUpdater listIndexUpdater(final Collection<? extends IndexUpdater> updaters) {
      IndexUpdater result = new IndexUpdater() {

         @Override public void add(Number value, Integer entityId, Integer rowCount)
         {
            for (IndexUpdater item : updaters) {
               item.add(value, entityId, rowCount);
            }
         }

         @Override public void finish()
         {
            for (IndexUpdater item : updaters) {
               item.finish();
            }
         }
      };
      return result;
   }
}
