// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Hadrien Chauvin

package org

import org.apache.spark.sql.SparkSession
import org.scalatest.Tag

package object opendata {

  /**
   * Tag for tests that fetch from external sources.  Fetching for external sources is
   * necessary to ensure there is no API drift.
   */
  private[opendata] object ExternalSourceTest extends Tag("org.opendata.tags.ExternalSourceTest")

  /** Trait to expose a lazily built spark session to test suites. */
  private[opendata] trait SparkSessionTestWrapper {

    lazy val spark: SparkSession = {
      SparkSession
        .builder()
        .master("local")
        .appName("spark test example")
        .getOrCreate()
    }
  }
}
