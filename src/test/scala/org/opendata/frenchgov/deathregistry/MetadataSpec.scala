// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Hadrien Chauvin

package org.opendata.frenchgov.deathregistry

import org.opendata.ExternalSourceTest
import org.opendata.frenchgov.deathregistry.Parser.bytesPerRecord
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import reactor.core.scala.publisher.SFlux

import scala.io.Source
import scala.util.parsing.json.JSON

class MetadataSpec extends AnyFlatSpec with Matchers {
  behavior of "dataURLS"

  it should "extract data URLs" in {
    val rawMetadataJson = Source
      .fromResource("org/opendata/frenchgov/deathregistry/metadata.txt")
      .mkString
    val rawMetadata = JSON.parseFull(rawMetadataJson).get
    val urls = Metadata.dataURLs(rawMetadata)

    urls should be(
      Seq(
        "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20200310-151524/deces-2020-m02.txt",
        "https://static.data.gouv.fr/resources/fichier-des-personnes-decedees/20200207-113656/deces-2020-m01.txt"
      )
    )
  }

  behavior of "requests"

  it should "not split a file smaller than the chunk size" in {
    val recordsPerChunk = 10
    val bytesPerChunk = recordsPerChunk * bytesPerRecord
    val contentLengths =
      Seq(("/deces-2012.txt", bytesPerRecord * (recordsPerChunk / 2)))
    val requests =
      Metadata
        .requests(SFlux.fromIterable(contentLengths), recordsPerChunk)
        .collectSeq()
        .block()

    requests should be(
      Seq(
        Request(
          url = "/deces-2012.txt",
          start = 0,
          end = bytesPerChunk / 2 - 1,
          year = 2012,
          contentLength = bytesPerChunk / 2
        )
      )
    )
  }

  it should "not split a file larger than the chunk size" in {
    val recordsPerChunk = 10
    val bytesPerChunk = recordsPerChunk * bytesPerRecord
    val contentLength: Int =
      math.round(bytesPerRecord * (recordsPerChunk * 2.5)).toInt
    val contentLengths = Seq(("/deces-2013.txt", contentLength))
    val requests =
      Metadata
        .requests(SFlux.fromIterable(contentLengths), recordsPerChunk)
        .collectSeq()
        .block()

    val expectedStartEnd: Seq[(Int, Int)] = Seq(
      (0, bytesPerChunk - 1),
      (bytesPerChunk, bytesPerChunk * 2 - 1),
      (bytesPerChunk * 2, contentLength - 1)
    )
    val expectedRequests = expectedStartEnd.map({
      case (start, end) =>
        Request(
          url = "/deces-2013.txt",
          start = start,
          end = end,
          year = 2013,
          contentLength = contentLength
        )
    })
    requests should be(expectedRequests)
  }

  behavior of "rawMetadata"

  it should "fetch external source" taggedAs (ExternalSourceTest) in {
    org.opendata.Utils.disableSSLChecks()

    val rawMetadata = Metadata.rawMetadata()
    val urls = Metadata.dataURLs(rawMetadata)
    urls should not be empty
  }

  behavior of "contentLengths"

  it should "get content lengths from external source" taggedAs (ExternalSourceTest) in {
    org.opendata.Utils.disableSSLChecks()

    val rawMetadata = Metadata.rawMetadata()
    val url = Metadata.dataURLs(rawMetadata).head
    val Seq((returnedUrl, contentLength)) =
      Metadata.contentLengths(Seq(url)).collectSeq().block()

    returnedUrl should be(url)
    contentLength should be > 0
  }
}
