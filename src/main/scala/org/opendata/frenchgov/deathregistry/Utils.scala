// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Hadrien Chauvin

package org.opendata.frenchgov.deathregistry

import java.time.LocalDate

/**
  * Helper methods.
  */
private[deathregistry] object Utils {

  /** Extracts the year from a URL. */
  def extractYear(url: String): Option[Int] = {
    val re = "^.*/deces-(\\d{4})(-[^/.]+)?\\.txt$".r
    url match {
      case re(year, _) => Some(year.toInt)
      case _           => None
    }
  }

  /** Parses a date */
  def parseDate(s: String): Option[LocalDate] = {
    val year = s.substring(0, 4).toInt
    val month = s.substring(4, 6).toInt
    val dayOfMonth = s.substring(6, 8).toInt
    if (year == 0) None
    else
      Some(
        LocalDate.of(
          year,
          if (month == 0) 1 else month,
          if (dayOfMonth == 0) 1
          else dayOfMonth
        )
      )
  }
}
