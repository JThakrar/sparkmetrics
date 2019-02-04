
# Spark Metrics

## Overview

## Pre-requisites
- Apache Spark 2.4.x
- Scala 2.11.x
- SBT

## How To Build, Deploy and Run

Download the project and build as follows.

```
git clone https://github.com/JThakrar/sparkmetrics.git

cd sparkmetrics

sbt assembly
```

## Background Info
As we know, architecturally when a Spark application program is run, there's a driver and one or more executors.
These architectural constructs can all be contained in a single process,
on a single machine as multiple JVM processes or across multiple JVM processes on one or more machines (or containers).

The Spark application program is instantiated on the driver and when the program involves constructs like
dataframes, datasets, datasources, the executors are involved by dispatching execution units to them.
At runtime, a program is logically composed of jobs, stages and tasks, with jobs and stages being
logical boundaries while tasks being the actual execution units that are dispatched to the executors.

Spark creates an event everytime a job stage or task is started or stopped, when the application starts and stops,
when executors are added/removed, etc. These events are broadcast on an "event bus". Additionally as executors are
running tasks, they send heartbeats and periodic updates (statistics/metrics) to the event bus.
These events can be saved to a file by setting a few Spark configuration parameters.

Additionally, Spark provides an API to listen for the Spark events. This project uses the API to to listen
for the following events:
- job (start and) end
- stage end
- application end

The project provides structures for runtime  metrics for stages, jobs and the application itself
and a mechanism to save them to a specific destination like file, console or database using a
destination-specific metric writer.

Conceptually, metrics can be output to a destination by creating a SparkEventListener instance supplied with a
destination specific writer.

## Sample Usage
Lets consider a simple Spark application that reads parquet data from a directory and computes an aggregation.

val data = spark.read.parquet("/input/dir/path")
val summary = data.groupBy("region", "product").agg(sum("volume"), sum("amt"))
summary.write.parquet("/output/dir/path")

Sparkmetrics can be applied to the above as follows:

val metricWriter = new FileMetricWriter("/metrics/dir/path")
val sparkEventListener = new SparkEventListener(spark, metricWriter)
spark.sparkContext.addSparkListener(sparkEventListener)
val data = spark.read.parquet("/input/dir/path")
val summary = data.groupBy("region", "product").agg(sum("volume"), sum("amt"))
summary.write.parquet("/output/dir/path")

This will capture spark events and the details will be available in files as follows:
application summary: <app_id>.json
job metrics: <app_id>.<job_id>.json
stage metrics: <app_id>.<job_id>.json

As can be seen, by just adding 3 lines to an existing spark application, you now have a lot of metrics -
especially from the stages available to you for troubleshooting and analysis.

## Why Sparkmetrics?

Having these metrics allows one to quantitatively answer the impact of following and more:
- Adding of more executors or memory
- Number or size of files on processing time and resources
- Repartitioning
- Caching
- Code restructuring
- Changes to input/output data sources or format
- Data volume changes
- Cluster resources and configuration

Additionally, having such metrics as baseline allows one to troubleshoot when there is a large deviation in runtime duration, etc.

And having sufficient amount of historical data allows one to predict job completion time - which is an often asked question -
since Spark jobs on YARN always show 10% completion.



