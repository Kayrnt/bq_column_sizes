package fr.kayrnt.logic

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions, DatasetId, TableId}
import fr.kayrnt.BQColumnSizesOpts
import fr.kayrnt.model.ColumnSize
import fr.kayrnt.reader.{Partitions, Tables}
import fr.kayrnt.writer.OutputWriter
import fr.kayrnt.writer.impl.{BigQueryOutputWriter, CsvOutputWriter}

import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe.Try

object BQColumnSizesLogic {

  def run(options: BQColumnSizesOpts): Unit = {

    val concurrentQueriesSemaphore: IO[Semaphore[IO]] =
      Semaphore[IO](options.maxConcurrentQueries.map(_.toLong).getOrElse(4))
    val outputFilePath = options.outputFilePath.getOrElse("size.csv")

    def withResources(
        writer: OutputWriter
    )(body: (Semaphore[IO], ColumnSize => IO[_]) => IO[_]): IO[_] =
      concurrentQueriesSemaphore.flatMap { queriesSem =>
        writer.client.use { c =>
          body(queriesSem, c.write).map { _ =>
            c.close()
          }
        }
      }

    val bq: BigQuery = BigQueryOptions
      .newBuilder()
      .setCredentials(GoogleCredentials.getApplicationDefault)
      .setProjectId(options.executionProjectId.getOrElse(options.projectId))
      .build()
      .getService

    val outputWriter = options.outputWriter.getOrElse("csv")

    val outputProject = options.outputProject.getOrElse(options.projectId)

    val outputDataset = options.outputDataset.getOrElse {
      if (outputWriter == "bq")
        throw new IllegalArgumentException("BQ output dataset expected with BQ output")
      else ""
    }

    val outputTable = options.outputTable.getOrElse {
      if (outputWriter == "bq")
        throw new IllegalArgumentException("BQ output table expected with BQ output")
      else ""
    }

    val partition = options.offset.map(o => scala.concurrent.duration.Duration(o)) match {
      case Some(offset) => options.partition.map(p => Partitions.applyOffset(p, offset))
      case None         => options.partition
    }

    val outputWriterImpl = outputWriter match {
      case "bq" =>
        new BigQueryOutputWriter(
          bq,
          outputProject,
          outputDataset,
          outputTable,
          outputFilePath,
          partition
        )
      case _ => new CsvOutputWriter(outputFilePath, partition)
    }

    (options.dataset, options.table) match {
      case (None, None) =>
        withResources(outputWriterImpl) { (sem, csvWriter) =>
          (for {
            dataset <- bq.listDatasets(options.projectId).iterateAll().asScala
            table   <- bq.listTables(dataset.getDatasetId).iterateAll().asScala
          } yield Tables.analyzeTable(
            bq,
            sem,
            options.projectId,
            table,
            csvWriter,
            partition
          )).toList.sequence
        }.unsafeRunSync()
      case (Some(dataset), None) =>
        withResources(outputWriterImpl) { (sem, csvWriter) =>
          (for {
            table <- bq.listTables(DatasetId.of(options.projectId, dataset)).iterateAll().asScala
          } yield Tables.analyzeTable(
            bq,
            sem,
            options.projectId,
            table,
            csvWriter,
            partition
          )).toList.sequence
        }.unsafeRunSync()
      case (Some(dataset), Some(table)) =>
        withResources(outputWriterImpl) { (sem, csvWriter) =>
          val tableData = bq.getTable(TableId.of(options.projectId, dataset, table))
          Tables.analyzeTable(bq, sem, options.projectId, tableData, csvWriter, partition)
        }.unsafeRunSync()
      case (_, Some(_)) =>
        throw new IllegalArgumentException(
          s"Expected dataset details provided but option wasn't provided"
        )
      case _ =>

    }

  }

}
