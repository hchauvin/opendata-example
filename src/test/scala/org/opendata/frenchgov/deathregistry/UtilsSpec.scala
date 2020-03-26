// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Hadrien Chauvin

package org.opendata.frenchgov.deathregistry

import java.time.LocalDate

import org.opendata.frenchgov.deathregistry.Utils.parseDate
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UtilsSpec extends AnyFlatSpec with Matchers {
  behavior of "extractYear"

  it should "extract the year" in {
    Utils.extractYear("/deces-2012.txt") should be(Some(2012))
    Utils.extractYear("/deces-2015-m02.txt") should be(Some(2015))
    Utils.extractYear("/deces-2016-mt01.txt") should be(Some(2016))
    Utils.extractYear("__invalid__") should be(None)
  }

  behavior of "parseDate"

  it should "extract the date" in {
    parseDate("19350530") should be(Some(LocalDate.of(1935, 5, 30)))
    parseDate("20200224") should be(Some(LocalDate.of(2020, 2, 24)))
    parseDate("00000000") should be(None)
    parseDate("20200000") should be(Some(LocalDate.of(2020, 1, 1)))
    parseDate("20201000") should be(Some(LocalDate.of(2020, 10, 1)))
  }

  it should "build requests" in {}
}
