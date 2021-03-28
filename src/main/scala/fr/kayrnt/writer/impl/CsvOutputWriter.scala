package fr.kayrnt.writer.impl

import cats.effect.std.Semaphore
import cats.effect.{IO, Resource}
import com.github.tototoshi.csv.CSVWriter
import com.typesafe.scalalogging.LazyLogging
import fr.kayrnt.model.ColumnSize
import fr.kayrnt.writer.{OutputWriter, OutputWriterClient}
import scala.util.chaining.scalaUtilChainingOps

import java.io.File

class CsvOutputWriter(outputFilePath: String, partitionOpt: Option[String])
    extends OutputWriter
    with LazyLogging {

  private val csvSemaphore = Semaphore[IO](1)
  private val columnsName =
    List("project_id", "dataset", "table", "field", "partition", "size_in_bytes")
      .pipe(l => if (partitionOpt.isDefined) "date" :: l else l)

  protected val csvClient: Resource[IO, OutputWriterClient] = Resource.fromAutoCloseable {
    IO {
      val f = new File(outputFilePath)
      if (!f.exists())
        f.createNewFile()
      val w = CSVWriter.open(f)
      w.writeRow(columnsName)
      new OutputWriterClient {

        override def close(): Unit = w.close()

        override def write(columnSize: ColumnSize): IO[_] =
          csvSemaphore.flatMap { sem =>
            sem.permit.use { _ =>
              IO {
                val row =
                  columnSize.toList
                    .pipe(l => partitionOpt.map(p => p :: l).getOrElse(l))
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
