package io.dylemma.sandbox

import io.dylemma.sandbox.MyMultipart.{ PartChunk, PartEnd, PartStart }

import java.nio.charset.StandardCharsets
import scala.concurrent.duration._

import cats.effect.std.Supervisor
import cats.effect.{ ExitCode, IO, IOApp }
import fs2.io.file.Path
import fs2.{ Chunk, Stream }
import org.http4s.multipart.Part
import cats.syntax.apply._

object Main extends IOApp {
	def run(args: List[String]): IO[ExitCode] = {
		def mockPartStart(name: String) = PartStart(Part.formData[IO](name, "<ignored>").headers)

		def mockFilePartStart(name: String, filename: String) = PartStart(Part.fileData[IO](name, filename, Stream.empty).headers)

		def utf8Chunk(text: String) = PartChunk(Chunk.array(text.getBytes(StandardCharsets.UTF_8)))

		// Pretend we have access to `org.http4s.multipart.MultipartParser.parseEvents`
		// and we've fed a `multipart/form-data` entity through that pipe.
		val exampleEvents = fs2.Stream
			.emits(List(
				mockPartStart("foo"),
				utf8Chunk("bar"),
				PartEnd,
				mockFilePartStart("file", "bar.txt"),
				utf8Chunk("""{ "hello": """),
				utf8Chunk(""""world" }"""),
				utf8Chunk("""{ "hello": """),
				utf8Chunk(""""world" }"""),
				utf8Chunk("""{ "hello": """),
				utf8Chunk(""""world" }"""),
				PartEnd,
				mockPartStart("bop"),
				utf8Chunk("bippity "),
				utf8Chunk("boppity "),
				utf8Chunk("boo"),
				PartEnd,
				mockFilePartStart("baz", "baz.txt"),
				utf8Chunk("zoomy"),
				utf8Chunk("doomy"),
				PartEnd,
			))
			.covary[IO]
			.metered(50.millis)
			.evalTap(e => IO.println(s"[in] $e"))

		// A MultipartReceiver decides how to consume each part, then assembles the
		// outputs from each individual part into an overall result. In this way,
		// you can define a receiver for a specific field to expect a specific format,
		// or use `auto` to consume all fields in a generic way.
		val receiver: MultipartReceiver[IO, (String, Path, Map[String, MultipartReceiver.FieldValue])] = (
			MultipartReceiver[IO].textPart("foo")(_.readString
				.tapStart(IO.println("  [foo] start receiving text"))
				.tapResult(s => IO.println(s"  [foo] finished with value $s"))
				.tapRelease(IO.println("  [foo] release")),
			),
			MultipartReceiver[IO].filePart("file")(_.toTempFile
				// .withSizeLimit(19)
				.tapStart(IO.println("  [file] start receiving file"))
				.tapResult(f => IO.println(s"  [file] finished writing to $f"))
				.tapRelease(IO.println("  [file] delete temp file")),
			),
			MultipartReceiver[IO].auto.transformParts(_
				.tapStart(IO.println("  [generic] start receiving data"))
				.tapResult(a => IO.println(s"  [generic] finished with value $a"))
				.tapRelease(IO.println("  [generic] released resource")),
			),
		).tupled

		Supervisor[IO].use { supervisor =>
			MyMultipart.decodeMultipart(
				exampleEvents,
				supervisor,
				receiver,
			).biSemiflatMap(
				err => IO.println(s"[FAILURE] $err").andWait(1.second),
				a => IO.println(s"[SUCCESS] $a").andWait(1.second),
			).merge.as(ExitCode.Success)
		} <* IO.println("closed supervisor")
	}

}
