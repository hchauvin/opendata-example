#!/usr/bin/env amm

// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Hadrien Chauvin

val jarPath = java.nio.file.FileSystems.getDefault()
  .getPath("target/scala-2.12/opendata-assembly-0.1.jar")
  .toAbsolutePath()
interp.load.cp(ammonite.ops.Path(jarPath))

@

import org.apache.spark.sql.SparkSession

val spark: SparkSession = {
  SparkSession
    .builder()
    .master("spark://localhost:7077")
    .appName("spark test example")
    .getOrCreate()
}

spark.read
  .format("org.opendata.frenchgov.deathregistry")
  .option("max.partitions", "1")
  .option("records.per.chunk", "2")
  .option("records.per.partition", "4")
  .load()
  .toDF()
  .first()

