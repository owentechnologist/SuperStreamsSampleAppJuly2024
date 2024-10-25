This sample Application shows the 0.3.5 version of the Redis Enterprise Streams framework being utilized by a Jedis client.

This streams project enhances Redis Streams making it easier to scale and utilize Redis Streams.

Ensure you have:
* A modern instance of Redis running that includes the Redis Search module (I suggest version 7.4 and above) https://redis.io/try-free/ 


* Execute the mvn command with necessary args to connect to your Redis instance

Example:

``` 
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redis-18000.homelabwin.local --port 18000 --songtitle 'love me tender' --ackmessage true"
```
