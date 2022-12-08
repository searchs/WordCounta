##  Word Count using Spark Streaming
As part of developing a course on Spark Streaming, this example is a good start for the students and should give insight into basic Spark Streaming setup



### Dev Tools
- Apache Spark (Core, SQL)
- Scala (at least 2.13.10)
- SBT (similar to Maven - for those that do not know it already)
- Operating System: MacOS, Linux, Windows
- Local emitter - simulate stream of words for app to consume: 
  ```ncat -lk 9999```