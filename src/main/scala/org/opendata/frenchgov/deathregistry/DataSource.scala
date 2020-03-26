// SPDX-License-Identifier: MIT
// Copyright (c) 2020 Hadrien Chauvin

package org.opendata.frenchgov.deathregistry

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{
  SupportsRead,
  Table,
  TableCapability,
  TableProvider
}
import org.apache.spark.sql.connector.read.{
  Batch,
  InputPartition,
  PartitionReader,
  PartitionReaderFactory,
  Scan,
  ScanBuilder
}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import reactor.core.scala.publisher.SFlux

/**
  * Abstract Spark data source for the death registry.
  *
  * The abstract base class is used both by [[DefaultSource]] and for testing purposes.
  */
private[deathregistry] abstract class DataSource extends TableProvider {

  /** Returns the partitions. */
  def partitions(
      options: CaseInsensitiveStringMap
  ): () => Seq[DataSource.CustomInputPartition]

  override def getTable(options: CaseInsensitiveStringMap) =
    new DataSource.CustomTable(partitions(options))
}

private[deathregistry] object DataSource {
  val customSchema: StructType = {
    import org.apache.spark.sql.types._

    StructType(
      StructField("sex", StringType, nullable = false) ::
        StructField("birthdate", DateType, nullable = true) ::
        StructField("deathDate", DateType, nullable = true) :: Nil
    )
  }

  abstract class CustomInputPartition extends InputPartition {
    def get(): SFlux[Death]
  }

  class CustomTable(partitions: () => Seq[CustomInputPartition])
      extends Table
      with SupportsRead {
    import scala.collection.JavaConverters._

    override def name(): String = {
      "deaths"
    }

    override def schema(): StructType = customSchema

    override def capabilities(): java.util.Set[TableCapability] = {
      Set(TableCapability.BATCH_READ).asJava
    }

    override def newScanBuilder(options: CaseInsensitiveStringMap) =
      new CustomScanBuilder(partitions)
  }

  class CustomScanBuilder(partitions: () => Seq[CustomInputPartition])
      extends ScanBuilder {
    override def build() = new CustomScan(partitions)
  }

  class CustomScan(partitions: () => Seq[CustomInputPartition]) extends Scan {
    override def readSchema(): StructType = customSchema

    override def toBatch = new CustomBatch(partitions)
  }

  class CustomBatch(partitions: () => Seq[CustomInputPartition]) extends Batch {
    private lazy val inputPartitions: Seq[CustomInputPartition] = {
      partitions()
    }

    override def planInputPartitions(): Array[InputPartition] =
      inputPartitions.toArray[InputPartition]

    override def createReaderFactory(): PartitionReaderFactory =
      new CustomPartitionReaderFactory()
  }

  class CustomPartitionReaderFactory extends PartitionReaderFactory {
    override def createReader(
        partition: InputPartition
    ): PartitionReader[InternalRow] =
      new CustomPartitionReader(partition.asInstanceOf[CustomInputPartition])
  }

  class CustomPartitionReader(partition: CustomInputPartition)
      extends PartitionReader[InternalRow] {
    private lazy val stream = partition
      .get()
      .asJava()
      .toStream() // Java streams are closeable, Scala streams are not
    private lazy val iterator = stream.iterator()

    override def next(): Boolean = iterator.hasNext

    override def get(): InternalRow = {
      val record = iterator.next()
      InternalRow(
        UTF8String.fromString(record.sex),
        record.birthdate.map { _.toEpochDay.toInt }.orNull,
        record.deathDate.map { _.toEpochDay.toInt }.orNull
      )
    }

    override def close(): Unit = stream.close()
  }
}
