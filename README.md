# Spark-lever

Spark-lever is based on Spark Streaming,it is a proactive capability-aware load balancing system for batch stream processing on heterogeneous clusters.

Lever derives the load balancing model from running time states information of previous jobs and measure worker nodes’ heterogeneity in computational capabilities by combining processing speed of previous job and hardware resources. When receiving input stream load, it distributes load in advance according to the processing capabilities that each worker node has, not in processing stage. Lever treats the problem of how to distribute load between worker nodes as a bin packing problem with divisible item sizes. An Optimized First Fit Decreasing (OFFD) algorithm is proposed for making load balancing decisions between overloaded worker nodes and underloaded worker nodes.  

## How to use ?

Now, Spark-lever is based on Spark-1.3.0, so you can get the documentation from Spark homepage (http://spark.apache.org/)

Have fun!