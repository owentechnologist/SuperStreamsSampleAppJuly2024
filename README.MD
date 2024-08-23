This sample Application shows the 0.3.1.2 version of the Redis Enterprise Streams framework being utilized by a Jedis client.

As the framework matures, I hope to keep this sample up to date as well.

***
In order to run this example, you will need to:

* install the necessary client-side jar into your local maven repo:

```
mvn org.apache.maven.plugins:maven-install-plugin:2.5.2:install-file -Dfile=redis-streams-java-0.3.1.2.jar
```

* Have a modern instance of Redis running that includes the Redis Search module (I suggest version 7.4 and above)

* Execute the mvn command with necessary args to connect to your Redis instance

Example:

``` 
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redis-FIXME.c309.us-east-2-1.ec2.cloud.redislabs.com --port FIXME --password WqedzFIXMEYYxzI --songtitle 'love me tender' --ackmessage true"
```
