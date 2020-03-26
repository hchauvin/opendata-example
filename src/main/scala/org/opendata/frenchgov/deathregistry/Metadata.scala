// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Hadrien Chauvin

package org.opendata.frenchgov.deathregistry

import java.util.Calendar

import org.opendata.Utils.httpClient
import reactor.core.scala.publisher.SFlux
import reactor.netty.http.client.HttpClientResponse

import scala.util.parsing.json.JSON

private[deathregistry] object Metadata {

  /** Reads raw metadata */
  def rawMetadata(): Any = {
    // FIXME: Remove when Certigna is added to cacerts
    // https://github.com/habitat-sh/core-plans/issues/1488
    org.opendata.Utils.disableSSLChecks()

    import scala.io.Source.fromURL

    val source = fromURL(
      "https://www.data.gouv.fr/api/1/datasets/fichier-des-personnes-decedees/"
    )
    try {
      val json = source.mkString
      JSON.parseFull(json).get
    } finally {
      source.close()
    }
  }

  /** Gets the URLS from which the data should be read. */
  def dataURLs(rawMetadata: Any): Seq[String] = {
    val currentYear = Calendar.getInstance().get(Calendar.YEAR)
    rawMetadata
      .asInstanceOf[Map[String, Any]]("resources")
      .asInstanceOf[List[Any]]
      .map(resourceAny => {
        val resource = resourceAny.asInstanceOf[Map[String, Any]]
        val title: String = resource("title").asInstanceOf[String]
        val url = resource("url").asInstanceOf[String]
        if (title.startsWith("deces-" + currentYear + "-m")
            || title.matches("deces-(\\d{4}).txt")) {
          Some(url)
        } else {
          None
        }
      })
      .collect { case Some(value) => value }
  }

  /** Gets the content lengths of all the files. */
  def contentLengths(urls: Iterable[String]): SFlux[(String, Int)] = {
    val responses: SFlux[(String, HttpClientResponse)] = SFlux.zip(
      SFlux
        .fromIterable(urls),
      SFlux
        .fromIterable(urls)
        .concatMap(url => httpClient.head().uri(url).response())
    )

    responses.map({
      case (url, resp) => {
        if (resp.status().code() != 200) {
          throw new RuntimeException(
            "non-200 error code: " + resp.status().code()
          )
        }
        val acceptRanges = resp.responseHeaders().get("Accept-Ranges")
        if (acceptRanges != "bytes") {
          throw new RuntimeException(
            "expected bytes Accept-Ranges, got: " + acceptRanges
          )
        }
        val contentLength = resp.responseHeaders().get("Content-Length").toInt
        (url, contentLength)
      }
    })
  }

  /** Builds the HTTP requests. */
  def requests(
      contentLengths: SFlux[(String, Int)],
      recordsPerChunk: Int
  ): SFlux[Request] = {
    val bytesPerChunk = Parser.bytesPerRecord * recordsPerChunk
    contentLengths.flatMapIterable({
      case (url, contentLength) => {
        Range(
          0,
          math
            .ceil(contentLength.asInstanceOf[Double] / bytesPerChunk)
            .asInstanceOf[Int]
        ).map(i =>
          Request(
            url = url,
            start = i * bytesPerChunk,
            end = math.min((i + 1) * bytesPerChunk - 1, contentLength - 1),
            contentLength = contentLength,
            year = Utils
              .extractYear(url)
              .getOrElse(throw new RuntimeException("unexpected url " + url))
          )
        )
      }
    })
  }
}
