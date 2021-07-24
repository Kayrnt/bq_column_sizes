package fr.kayrnt.writer.impl

import cats.effect.std.Semaphore
import cats.effect.{IO, Resource}
import com.github.tototoshi.csv.CSVWriter
import com.google.cloud.bigquery.JobInfo.WriteDisposition
import com.typesafe.scalalogging.LazyLogging
import fr.kayrnt.model.ColumnSize
import fr.kayrnt.writer.{OutputWriter, OutputWriterClient}

import scala.util.chaining.scalaUtilChainingOps
import java.io.File

class CsvOutputWriter(outputFilePath: String, writeDisposition: WriteDisposition)
    extends OutputWriter
    with LazyLogging {

  private val csvSemaphore = Semaphore[IO](1)
  private val columnsName =
    List(
      "job_partition",
      "project_id",
      "dataset",
      "table",
      "field",
      "table_partition",
      "size_in_bytes"
    )

  protected val csvClient: Resource[IO, OutputWriterClient] = Resource.fromAutoCloseable {
    IO {
      val f = new File(outputFilePath)
      if (!f.exists())
        f.createNewFile()

      if (f.isDirectory)
        throw new IllegalStateException(s"Output file ($outputFilePath) is directory")

      val fileIsEmpty = f.length() == 0
      if (!fileIsEmpty && writeDisposition != WriteDisposition.WRITE_EMPTY)
        throw new IllegalStateException(
          s"Output file ($outputFilePath) is not empty and write disposition write_empty expects empty/non existing file, use write_truncate to overwrite."
        )

      val w = CSVWriter.open(f, writeDisposition == WriteDisposition.WRITE_APPEND)
      w.writeRow(columnsName)
      new OutputWriterClient {

        override def close(): Unit = w.close()

        override def write(columnSize: ColumnSize): IO[_] =
          csvSemaphore.flatMap { sem =>
            sem.permit.use { _ =>
              IO {
                val row =
                  columnSize.toList
                logger.info("Write row : " + row)
                w.writeRow(row)
              }
            }
          }
      }
    }
  }

  override val client: Resource[IO, OutputWriterClient] = csvClient

}
