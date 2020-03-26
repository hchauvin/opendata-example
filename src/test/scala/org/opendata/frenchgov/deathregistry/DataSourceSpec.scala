// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Hadrien Chauvin

package org.opendata.frenchgov.deathregistry

import java.sql.Date

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.opendata.{ExternalSourceTest, SparkSessionTestWrapper}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DataSourceSpec
    extends AnyFlatSpec
    with Matchers
    with SparkSessionTestWrapper {
  behavior of "mock data source"

  it should "load mock data" in {
    val actualRows: Set[Death] = spark.read
      .format("org.opendata.frenchgov.deathregistry.TestDataSource")
      .load()
      .toDF()
      .collect()
      .map(row =>
        Death(
          row.getAs[String]("sex"),
          Option(row.getAs[Date]("birthdate")).map { _.toLocalDate },
          Option(row.getAs[Date]("deathDate")).map { _.toLocalDate }
        )
      )
      .toSet

    val expectedRows: Set[Death] = TestDataSource.data.values.flatten.toSet

    actualRows should equal(expectedRows)
  }

  behavior of "external data source options"

  it should "be parsed correctly" in {
    import scala.collection.JavaConverters._

    val parsedOptions = DefaultSource.parseOptions(
      new CaseInsensitiveStringMap(
        Map(
          "max.partitions" -> "1",
          "records.per.chunk" -> "10",
          "records.per.partition" -> "20"
        ).asJava
      )
    )

    parsedOptions should be(
      DefaultSource.Options(
        maxPartitions = Some(1),
        recordsPerChunk = 10,
        recordsPerPartition = 20
      )
    )
  }

  behavior of "external data source"

  it should "load external data" taggedAs (ExternalSourceTest) in {
    org.opendata.Utils.disableSSLChecks()

    spark.read
      .format("org.opendata.frenchgov.deathregistry")
      .option("max.partitions", "1")
      .option("records.per.chunk", "2")
      .option("records.per.partition", "4")
      .load()
      .toDF()
      .first()
  }
}
