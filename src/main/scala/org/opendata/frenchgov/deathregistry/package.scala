// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Hadrien Chauvin

package org.opendata.frenchgov

import java.time.LocalDate

import reactor.core.scala.publisher.SFlux

/**
  * deathregistry implements a parser and a Spark data source for the death
  * registry exposed at https://www.data.gouv.fr/en/datasets/fichier-des-personnes-decedees/
  * by the French government.
  *
  * [[requests]] and [[parseChunk]] are used under the hood by the Spark data source.
  * The Spark data source is used as follows:
  *
  * {{{
  * spark.read
  *   .format("org.opendata.frenchgov.deathregistry")
  *   .load()
  *   .toDF()
  *   .show()
  * }}}
  *
  * Note that the data source does not implement caching: the open data API, `data.gouv.fr`,
  * is called every time.
  */
package object deathregistry {

  /** Death occurence. */
  case class Death(
      sex: String,
      birthdate: Option[LocalDate],
      deathDate: Option[LocalDate]
  )

  object Sex {
    val Male = "1"
    val Female = "2"
  }

  /**
    * Request to the open data API.
    *
    * @param url The URL for the request.
    * @param start The offset (in bytes, inclusive) of the first byte to request in the remote
    *              file.
    * @param end The offset (in bytes, inclusive) of the last byte to request in the remote
    *            file.
    * @param year The year at which the file was produced.
    * @param contentLength Total number of bytes in the file.
    */
  case class Request(
      url: String,
      start: Int,
      end: Int,
      year: Int,
      contentLength: Int
  )

  /**
    * Creates a list of [[Request]] objects.
    *
    * The open data files are split into chunks with at most `recordsPerChunk` chunks.
    */
  def requests(recordsPerChunk: Int): SFlux[Request] = {
    val urls = Metadata.dataURLs(Metadata.rawMetadata())
    Metadata
      .requests(Metadata.contentLengths(urls), recordsPerChunk)
  }

  /**
    * Parses an open data file chunk.
    */
  def parseChunk(raw: String): Iterable[Death] = {
    Parser.parseChunk(raw)
  }
}
