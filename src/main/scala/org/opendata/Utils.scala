// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Hadrien Chauvin

package org.opendata

/**
  * Common helper methods.
  */
object Utils {

  /**
    * Disables SSL checks.
    *
    * @see https://stackoverflow.com/questions/28785609/scala-how-to-ignore-sslhandshakeexception
    */
  def disableSSLChecks(): Unit = {
    import javax.net.ssl._
    import java.security.cert.X509Certificate

    // Bypasses both client and server validation.
    object TrustAll extends X509TrustManager {
      override val getAcceptedIssuers: Array[X509Certificate] = null

      override def checkClientTrusted(
          x509Certificates: Array[X509Certificate],
          s: String
      ): Unit = ()

      override def checkServerTrusted(
          x509Certificates: Array[X509Certificate],
          s: String
      ): Unit = ()
    }

    // Verifies all host names by simply returning true.
    object VerifiesAllHostNames extends HostnameVerifier {
      override def verify(s: String, sslSession: SSLSession) = true
    }

    // SSL Context initialization and configuration
    val sslContext = SSLContext.getInstance("SSL")
    sslContext.init(null, Array(TrustAll), new java.security.SecureRandom())
    HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory)
    HttpsURLConnection.setDefaultHostnameVerifier(VerifiesAllHostNames)
  }

  import reactor.netty.http.client.HttpClient

  def httpClient: HttpClient = {
    import io.netty.handler.ssl.util.InsecureTrustManagerFactory
    import io.netty.handler.ssl.SslContextBuilder

    val sslContext = SslContextBuilder
      .forClient()
      .trustManager(InsecureTrustManagerFactory.INSTANCE)
      .build()

    HttpClient
      .create()
      // FIXME: Remove when Certigna is added to cacerts
      // https://github.com/habitat-sh/core-plans/issues/1488
      .secure { t => t.sslContext(sslContext) }
  }

  import reactor.core.scala.publisher.{SFlux, SMono}

  def httpGet(url: String, start: Int, end: Int): SMono[String] = {
    val range = s"bytes=$start-${end - 1}"
    val mono = httpClient
      .headers(headers => headers.set("Range", range))
      .get()
      .uri(url)
      .responseSingle((resp, content) => {
        resp.status().code() match {
          case 206 => content.asString
          case 416 => {
            val expectedRange = resp.responseHeaders().get("Content-Range")
            throw new RuntimeException(
              s"unsatisfiable range: $range -- expected: ${expectedRange}"
            )
          }
          case code =>
            throw new RuntimeException("unexpected non-200 status: " + code)
        }
      })
    SMono(mono)
  }

  /* Computes the SHA256 hex digest of a string. */
  def sha256(s: String): String = {
    import java.security.MessageDigest

    MessageDigest
      .getInstance("SHA-256")
      .digest(s.getBytes("UTF-8"))
      .map("%02x".format(_))
      .mkString
  }

  assert(sha256("hello") != sha256("world"))
  assert(sha256("hello") == sha256("hello"))
}
