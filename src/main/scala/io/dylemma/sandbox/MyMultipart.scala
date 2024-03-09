package io.dylemma.sandbox

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.DurationInt

import cats.arrow.FunctionK
import cats.data.EitherT
import cats.effect.std.Supervisor
import cats.effect.{ Deferred, ExitCode, IO, IOApp, Resource }
import cats.implicits.catsSyntaxMonadErrorRethrow
import cats.syntax.traverse._
import fs2.concurrent.Channel
import fs2.io.file.Files
import fs2.{ Chunk, Pull, Stream }
import org.http4s.Headers
import org.http4s.multipart.Part

object MyMultipart extends IOApp {
	sealed trait Event
	case class PartStart(headers: Headers) extends Event
	case class PartChunk(chunk: Chunk[Byte]) extends Event
	case object PartEnd extends Event

	def mockPartStart(name: String) = PartStart(Part.formData[IO](name, "<ignored>").headers)
	def mockFilePartStart(name: String, filename: String) = PartStart(Part.fileData[IO](name, filename, Stream.empty).headers)

	val exampleEvents = fs2.Stream.emits(List(
		mockPartStart("foo"),
		PartChunk(Chunk.array("bar".getBytes(StandardCharsets.UTF_8))),
		PartEnd,
		mockFilePartStart("file", "bar.txt"),
		PartChunk(Chunk.array("""{ "hello": """.getBytes(StandardCharsets.UTF_8))),
		PartChunk(Chunk.array(""""world" }""".getBytes(StandardCharsets.UTF_8))),
		PartChunk(Chunk.array("""{ "hello": """.getBytes(StandardCharsets.UTF_8))),
		PartChunk(Chunk.array(""""world" }""".getBytes(StandardCharsets.UTF_8))),
		PartChunk(Chunk.array("""{ "hello": """.getBytes(StandardCharsets.UTF_8))),
		PartChunk(Chunk.array(""""world" }""".getBytes(StandardCharsets.UTF_8))),
		PartEnd,
		mockPartStart("bop"),
		PartChunk(Chunk.array("bippity ".getBytes(StandardCharsets.UTF_8))),
		PartChunk(Chunk.array("boppity ".getBytes(StandardCharsets.UTF_8))),
		PartChunk(Chunk.array("boo".getBytes(StandardCharsets.UTF_8))),
		PartEnd,
	))

	def parseMultipartSupervised[A, E](
		events: Stream[IO, Event],
		supervisor: Supervisor[IO],
		decider: Part[IO] => Resource[IO, Either[E, A]],
	): EitherT[IO, E, Vector[A]] = {
		def pullPartStart(
			s: Stream[IO, Event],
		): Pull[IO, Either[E, A], Unit] = s.pull.uncons1.flatMap {
			case Some((ps: PartStart, tail)) =>

				Pull.eval { Channel.synchronous[IO, Chunk[Byte]] }.flatMap { ch =>

					// Create a `Resource` that can consume the Channel's `stream`
					// to produce an output or an error. Make sure that if the
					// Resource doesn't actually consume the whole channel stream
					// for whatever reason (errors, ignoring, or otherwise), that
					// we close the channel once an outcome is reached. Otherwise
					// the producer side of the channel (i.e. the main Pull loop)
					// would hang while trying to consume the stream of events.
					val receiverRes = decider(Part(ps.headers, ch.stream.unchunks))
						.evalTap(_ => ch.close)
						.handleErrorWith { (e: Throwable) =>
							Resource.eval(ch.close *> IO.raiseError(e))
						}

					// Pull all `PartChunk` events until a `PartEnd` event is seen,
					// pushing each Chunk into the channel for the resource to receive.
					val pullPart: Pull[IO, Nothing, Stream[IO, Event]] = pullUntilPartEnd(tail, ch)
						.handleErrorWith { err =>
							Pull.eval(ch.close) >> Pull.raiseError[IO](err)
						}
						.flatMap { nextPartsStream =>
							Pull.eval(ch.close) >> Pull.pure(nextPartsStream)
						}

					// Allocate the resource in the background, using the supervisor's lifetime to
					// release it later. Meanwhile, continue to pull chunk data and pipe it through
					// the shared channel, which should allow the resource to actually allocate.
					// When the Pull loop reaches the end of the current part, close the channel and
					// await the resource's allocation before deciding whether to continue pulling.
					Pull.eval(superviseResource(supervisor, receiverRes)).flatMap { resPromise =>
						pullPart.flatMap { restOfStream =>
							Pull.eval(resPromise.get.rethrow).flatMap { consumerResult =>
								val cont = consumerResult.fold(
									e => Pull.done,
									a => pullPartStart(restOfStream)
								)
								Pull.output1(consumerResult) >> cont
							}
						}
					}
				}

			case None =>
				Pull.done

			case Some((PartEnd, _)) =>
				Pull.raiseError[IO](new IllegalStateException("unexpected PartEnd"))

			case Some((PartChunk(_), _)) =>
				Pull.raiseError[IO](new IllegalStateException("unexpected PartChunk"))
		}

		def pullUntilPartEnd(s: Stream[IO, Event], ch: Channel[IO, Chunk[Byte]]): Pull[IO, Nothing, Stream[IO, Event]] = s.pull.uncons1.flatMap {
			case Some((PartEnd, tail)) =>
				Pull.pure(tail)
			case Some((PartChunk(chunk), tail)) =>
				Pull.eval(ch.send(chunk)) >> pullUntilPartEnd(tail, ch)
			case Some((PartStart(_), _)) | None =>
				Pull.raiseError[IO](new IllegalStateException("Missing PartEnd"))
		}

		// Translating the stream's effect to `Resource` and using `.eval`
		// causes all emitted resources to be allocated in a nested manner,
		// such that when the stream ends, all resources are allocated. We
		// take advantage of this by collecting them all to a Vector which
		// can be exposed to client code.
		pullPartStart(events)
			.stream
			.translate(EitherT.liftK[IO, E])
			.flatMap(r => Stream.eval(EitherT.fromEither[IO](r)))
			.compile
			.toVector
	}

	def superviseResource[A](supervisor: Supervisor[IO], res: Resource[IO, A]): IO[Deferred[IO, Either[Throwable, A]]] = {
		for {
			promise <- Deferred[IO, Either[Throwable, A]]
			// Allocate the resource "forever", until the supervisor wants to release itself.
			// When the resource allocates or fails to allocate, complete the "promise" so the
			// caller can un-block its `.get` call.
			foreverFiber <- supervisor.supervise[Nothing](
				res
					.attempt
					.evalTap(promise.complete)
					.rethrow
					.useForever
			)
		} yield promise
	}

	object NamedPart {
		def unapply[F[_]](part: Part[F]) = part.name
	}

	def run(args: List[String]): IO[ExitCode] = {
		Supervisor[IO].use { supervisor =>
			parseMultipartSupervised[(String, String), String](
				exampleEvents.covary[IO].metered(50.millis).evalTap(IO.println),
				supervisor,
				{
					case p @ NamedPart("foo") =>
						Resource.eval(IO { Right("ignore me" -> "blah") })
						//					Resource.pure(Left("oopsie"))
						// Resource.eval(IO.raiseErro
						// r(new Exception("boom!")))
						//					Resource.eval(IO.never[(String, String)])
					case p @ NamedPart(name) =>
						for {
							_ <- Resource.eval(IO.println(s"  [consumer] Start parsing $name body"))
							//						body <- s.through(fs2.text.utf8.decode).compile.resource.string
							tempFile <- Files[IO].tempFile
							_ <- Resource.eval(p.body.through(Files[IO].writeAll(tempFile)).compile.drain)
							//_ <- Resource.make(IO.println(s"Consumed $name as $body. (Allocated)"))(_ => IO.println(s"Release $name resource"))
							_ <- Resource.make(IO.println(s"  [consumer] Dumped $name body to $tempFile"))(_ => {
								IO.println(s"  [consumer] Releasing temp file $tempFile")
							})
						} yield Right(name -> tempFile.toString)
				},
			).value.flatMap { res =>
				IO.println(s"[RESULT] All parts hopefully allocated: $res").as(ExitCode.Success).andWait(1.second)
			}

		} <* IO.println("closed supervisor")
	}
}

