package fr.kayrnt.writer

import cats.effect.{IO, Resource}

trait OutputWriter {

  def client: Resource[IO, OutputWriterClient]

}
