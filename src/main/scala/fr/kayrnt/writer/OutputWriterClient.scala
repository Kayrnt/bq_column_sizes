package fr.kayrnt.writer

import cats.effect.IO
import fr.kayrnt.model.ColumnSize

trait OutputWriterClient extends AutoCloseable {

  def write(columnSize: ColumnSize): IO[_]

}
