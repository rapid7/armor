# ArmorWrite module.

## Deployment considerations:

The writer uses a lot of Heap based ByteBuffers. Because of this your writers could see native memory pressure that goes beyond the allocated heap max memory you set for your app. In order to have deterministic memory usage please consider these settings.

### 1. Set -Djdk.nio.maxCachedBufferSize=262144

This value is important such that the JVM will only cache in direct memory per thread based on the value passed in (in bytes). Even though the library uses heap based ByteBuffers, the JVM uses cache direct memory to push from the heap to disk. If this setting is not set then the cache goes unbounded per thread. Thus if you have a lot of threads using Heap ByteBuffers then the cache per thread is unbounded if this setting is not set.

### 2. Set the thread or pool settings according to your hardware.

Even with a cache limit for direct memory per thread, you still may see the process memory go way above the heap's max. This is most likely because the JVM is using direct memory to push the data from heap to disk (at /tmp). If you have lots of threads, then just by the average of time you probably see extreme spikes in a process's memory usage. To avoid these high peaks, you should tune down your thread/pool settings at a cost of decreased througput. 

__Note: The ArmorWriter assigns a thread per shard to process for writes. So if you have table of 16 shards and assign 10 threads, then only 10 out of the 16 shards will be updated concurrently.__

### 3. Decrease your batch size.

Similar to the thread count, you can also us a smaller batch size to ingest your data. For example say you issue a write request over 5000 entities, then
this requires the 5000 enttities and all their data to be pushed down to disk using the OS's direct memory. To avoid a large single request usage, you can easily break these requests into 5 write requests of 1K. This lower the memory requirements, but may slightly decrease your write speed performance.
