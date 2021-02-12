# ArmorWrite module.

## Deployment considerations:

The writer uses a lot of HeapBased byte buffers. Because of this your ingestors could see native memory pressure that goes beyond the allocated heap memory you set 
for java consumption. In order to have deterministic memory usage please set these two values accordingly.

### 1. Set -Djdk.nio.maxCachedBufferSize=262144

This value is important such that it will only cache in direct memory per thread for ByteBuffer direct memory. Even though the library uses heap memory, the direct 
memory underneath is used as a temporary cache when it writes to disk. Otherwise it will cache unbounded which can cause the appearance of a native memory leak.

### 2. Set the thread or pool settings according to your hardware.

Even with a cache cap for direct memory, you still may see the process RSS memory go way above the heap's max setting. This most likely because of the when the JVM is pushing
data from the heap (bytebuffer) to disk IO for the tmp disk storage. If you have a lot of threads, then just by average of you time you see extreme spikes in a process's
usage. To avoid these high peaks, you can tune down your thread/pool settings at a cost of decreased througput. Note the ArmorWriter assigns a thread per shard to handle.
So if you have table of 16 shaads and assign 10 threads, then only 10 out of the 16 shards will be updated at the same time.

### 3. Decrease your batch size.

Similar to the thread count you want to do more batching of a smaller set rather then one big batch. For example say you issue a write request of 5000 entities, then
this will eventually require the 5000 enttities and their data to be pushed down to disk using the direct memory for the transfer which will most likely see a spike in 
RSS usage. You can easily break these requests into 5 write requests of 1K. This should minimize the peak memory usage at a cost of slower write speeds.
