// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Hadrien Chauvin

package org.opendata.frenchgov.deathregistry

import java.time.LocalDate

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.io.Source

class ParserSpec extends AnyFlatSpec with Matchers {
  behavior of "parseChunk"

  it should "parse a chunk" in {
    val content = Source
      .fromResource("org/opendata/frenchgov/deathregistry/chunk.txt")
      .mkString
    val deaths = Parser.parseChunk(content).toSeq

    def date(year: Int, month: Int, dayOfMonth: Int) =
      Some(LocalDate.of(year, month, dayOfMonth))

    deaths should have length (13)
    deaths.take(2) should be(
      Seq(
        Death(Sex.Male, date(1935, 5, 30), date(2020, 2, 24)),
        Death(Sex.Female, date(1924, 7, 1), date(2020, 2, 10))
      )
    )
  }
}
