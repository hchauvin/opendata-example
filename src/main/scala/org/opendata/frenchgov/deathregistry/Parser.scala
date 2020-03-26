// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Hadrien Chauvin

package org.opendata.frenchgov.deathregistry

/**
  * Parser for the source files.
  *
  * The source files are fixed-width text files.
  *
  * Format is described at https://www.data.gouv.fr/en/datasets/fichier-des-personnes-decedees/.
  */
private[deathregistry] object Parser {

  /** `(start, end)` offsets (inclusive, exclusive) for all the columns.  */
  val startEnd: List[(Int, Int)] = {
    val widths: List[Int] = List(80, 1, 8, 5, 30, 30, 8, 5, 9)
    val indexes = widths.scan(0)(_ + _)
    indexes.dropRight(1).zip(indexes.drop(1))
  }

  /** Number of bytes per record */
  val bytesPerRecord = 200
  assert(startEnd.last._2 < bytesPerRecord)

  def parseChunk(raw: String): Iterable[Death] = {
    import util.control.Breaks._

    // This has been optimized for speed and memory usage (that's
    // why there is a "while" loop)
    Range(0, raw.length / bytesPerRecord).map(i => {
      var start = i * bytesPerRecord
      breakable {
        while (true) {
          val c = raw.charAt(start)
          if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
            break
          }
          start += 1
          if (start >= raw.length) {
            throw new RuntimeException("unexpected")
          }
        }
      }
      val values = Map(
        ('sex, startEnd(1)),
        ('birthdate, startEnd(2)),
        ('deathDate, startEnd(6))
      ).mapValues {
        case (cellStart, cellEnd) => {
          raw.substring(start + cellStart, start + cellEnd).trim()
        }
      }
      try {
        Death(
          sex = values('sex) match {
            case "1" => Sex.Male
            case "2" => Sex.Female
          },
          birthdate = Utils.parseDate(values('birthdate)),
          deathDate = Utils.parseDate(values('deathDate))
        )
      } catch {
        case e: Throwable => {
          val line = raw.substring(start, start + bytesPerRecord)
          throw new RuntimeException(
            s"cannot parse line '$line'; values: $values",
            e
          )
        }
      }
    })
  }
}
