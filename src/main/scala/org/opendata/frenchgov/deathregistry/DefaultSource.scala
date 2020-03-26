package org.opendata.frenchgov.deathregistry

// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Hadrien Chauvin

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.opendata.Utils.httpGet
import reactor.core.scala.publisher.SFlux

/**
  * Spark data source for the death registry.
  */
class DefaultSource extends DataSource {
  override def partitions(
      options: CaseInsensitiveStringMap
  ): () => Seq[DefaultSource.DefaultInputPartition] = () => {
    val parsedOptions = DefaultSource.parseOptions(options)
    val partitions = requests(parsedOptions.recordsPerChunk)
      .buffer(parsedOptions.recordsPerPartition / parsedOptions.recordsPerChunk)
      .map(x => DefaultSource.DefaultInputPartition(x))
    val filteredPartitions = parsedOptions.maxPartitions match {
      case None                => partitions
      case Some(maxPartitions) => partitions.take(maxPartitions)
    }
    filteredPartitions
      .collectSeq()
      .block()
  }
}

private[deathregistry] object DefaultSource {
  case class Options(
      maxPartitions: Option[Int],
      recordsPerChunk: Int,
      recordsPerPartition: Int
  ) {
    require(maxPartitions.getOrElse(1) >= 0)
    require(recordsPerChunk > 0)
    require(recordsPerPartition > 0)
    require(recordsPerChunk < recordsPerPartition)
  }

  val defaultRecordsPerPartition = 1000000
  val defaultRecordsPerChunk = 10000

  def parseOptions(options: CaseInsensitiveStringMap) = Options(
    maxPartitions = options.getInt("max.partitions", -1) match {
      case -1 => None
      case v  => Some(v)
    },
    recordsPerChunk =
      options.getInt("records.per.chunk", defaultRecordsPerChunk),
    recordsPerPartition =
      options.getInt("records.per.partition", defaultRecordsPerPartition)
  )

  case class DefaultInputPartition(requests: Seq[Request])
      extends DataSource.CustomInputPartition {
    override def get(): SFlux[Death] = {
      SFlux
        .fromIterable(requests)
        .flatMap(request => httpGet(request.url, request.start, request.end))
        .flatMapIterable(parseChunk)
    }
  }
}
