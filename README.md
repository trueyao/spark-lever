# Spark-lever

Spark-lever is based on Spark Streaming, it is a pre-scheduling straggler mitigation framework for batched stream processing.

Lever first identifies potential stragglers and evaluates nodes' capacity by analyzing execution information of historical jobs. Then, Lever carefully pre-schedules job input data to each node before task scheduling so as to mitigate the potential stragglers.

## How to use ?

Now, Spark-lever is based on Spark-1.3.0, so you can get the documentation from Spark homepage (http://spark.apache.org/). This is only the source code of Lever. So you need to compile Lever using mvn or sbt first. Then, you can deploy Lever on a heterogeneous cluster or data skew scenarios to test its performance.

Have fun!
