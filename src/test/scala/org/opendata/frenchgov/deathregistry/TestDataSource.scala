// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Hadrien Chauvin

package org.opendata.frenchgov.deathregistry

import java.time.LocalDate

import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.opendata.frenchgov.deathregistry.DataSource.CustomInputPartition
import reactor.core.scala.publisher.SFlux

/**
 * Test data source that does not fetch the data from the external server.
 * Instead, a mock data set is used.
 */
class TestDataSource extends DataSource {
  override def partitions(options: CaseInsensitiveStringMap) =
    () => TestDataSource.data.keys.map(TestDataSource.CustomTestPartition).toSeq
}

private[deathregistry] object TestDataSource {
  val data: Map[String, Seq[Death]] = {
    def date(year: Int, month: Int, dayOfMonth: Int) =
      Some(LocalDate.of(year, month, dayOfMonth))

    Map(
      "A" -> Seq(
        Death(Sex.Male, date(1920, 1, 10), date(2012, 2, 9)),
        Death(Sex.Female, None, date(2012, 2, 9))
      ),
      "B" -> Seq(
        Death(Sex.Male, date(1920, 1, 10), date(2013, 2, 9)),
        Death(Sex.Male, date(1920, 1, 10), None)
      )
    )
  }

  case class CustomTestPartition(name: String) extends CustomInputPartition {
    def get() = SFlux.fromIterable(TestDataSource.data(name))
  }
}
