package io.dylemma.sandbox

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.DurationInt

import cats.arrow.FunctionK
import cats.effect.{ ExitCode, IO, IOApp, Resource }
import cats.syntax.traverse._
import fs2.concurrent.Channel
import fs2.io.file.Files
import fs2.{ Chunk, Pull, Stream }

object MyMultipart extends IOApp {
	sealed trait Event
	case class PartStart(header: String) extends Event
	case class PartChunk(chunk: Chunk[Byte]) extends Event
	case object PartEnd extends Event

	val exampleEvents = fs2.Stream.emits(List(
		PartStart("foo"),
		PartChunk(Chunk.array("bar".getBytes(StandardCharsets.UTF_8))),
		PartEnd,
		PartStart("bar"),
		PartChunk(Chunk.array("""{ "hello": """.getBytes(StandardCharsets.UTF_8))),
		PartChunk(Chunk.array(""""world" }""".getBytes(StandardCharsets.UTF_8))),
		PartChunk(Chunk.array("""{ "hello": """.getBytes(StandardCharsets.UTF_8))),
		PartChunk(Chunk.array(""""world" }""".getBytes(StandardCharsets.UTF_8))),
		PartChunk(Chunk.array("""{ "hello": """.getBytes(StandardCharsets.UTF_8))),
		PartChunk(Chunk.array(""""world" }""".getBytes(StandardCharsets.UTF_8))),
		PartEnd,
		PartStart("bop"),
		PartChunk(Chunk.array("bippity ".getBytes(StandardCharsets.UTF_8))),
		PartChunk(Chunk.array("boppity ".getBytes(StandardCharsets.UTF_8))),
		PartChunk(Chunk.array("boo".getBytes(StandardCharsets.UTF_8))),
		PartEnd,
	))

	def parseMultipart[A, E](
		events: Stream[IO, Event],
		decider: PartStart => Stream[IO, Byte] => Resource[IO, Either[E, A]],
	): Resource[IO, Either[E, Vector[A]]] = {
		def pullPartStart(
			s: Stream[IO, Event],
		): Pull[IO, Resource[IO, Either[E, A]], Unit] = s.pull.uncons1.flatMap {
			case Some((ps: PartStart, tail)) =>

				Pull.eval { Channel.synchronous[IO, Chunk[Byte]] }.flatMap { ch =>

					// Create a `Resource` that can consume the Channel's `stream`
					// to produce an output or an error. Make sure that if the
					// Resource doesn't actually consume the whole channel stream
					// for whatever reason (errors, ignoring, or otherwise), that
					// we close the channel once an outcome is reached. Otherwise
					// the producer side of the channel (i.e. the main Pull loop)
					// would hang while trying to consume the stream of events.
					val receiverRes = decider(ps)(ch.stream.unchunks)
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

					// Run the receiver in the background to avoid deadlocking the Channel.
					// When the `pullPart` finishes, wait for the resource to be allocated
					// by joining the fiber created by `.allocatedCase.start`. We defer the
					// release of the resource by emitting a *new* resource as a Pull.output1,
					// attaching the original resource's release logic to the result that was
					// allocated. If the result was a Left, abort the main Pull; otherwise recurse.
					Pull.eval(receiverRes.allocatedCase.start).flatMap { consumerFiber =>
						pullPart.flatMap { restOfStream =>
							Pull.eval(consumerFiber.joinWithNever).flatMap {
								case (consumerResult, releaseConsumer) =>
									val resultWrapped = Resource.makeCase(IO.pure(consumerResult)) { (_, exitCase) => releaseConsumer(exitCase) }
									val cont = consumerResult match {
										case Left(_) => Pull.done
										case Right(_) => pullPartStart(restOfStream)
									}
									Pull.output1(resultWrapped) >> cont
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
			.translate(new FunctionK[IO, Resource[IO, *]] {
				def apply[A](fa: IO[A]): Resource[IO, A] = Resource.eval(fa)
			})
			.flatMap(Stream.eval)
			.compile
			.toVector
			.map(_.sequence)
	}

	val partsParsed = parseMultipart(
		exampleEvents.covary[IO].metered(50.millis).evalTap(IO.println),
		{
			case PartStart("foo") =>
				(s: Stream[IO, Byte]) => {
					Resource.eval(IO { Right("ignore me" -> "blah") })
					//					Resource.pure(Left("oopsie"))
					// Resource.eval(IO.raiseErro
					// r(new Exception("boom!")))
					//					Resource.eval(IO.never[(String, String)])
				}
			case PartStart(name) =>
				(s: Stream[IO, Byte]) =>
					for {
						_ <- Resource.eval(IO.println(s"  [consumer] Start parsing $name body"))
						//						body <- s.through(fs2.text.utf8.decode).compile.resource.string
						tempFile <- Files[IO].tempFile
						_ <- Resource.eval(s.through(Files[IO].writeAll(tempFile)).compile.drain)
						//_ <- Resource.make(IO.println(s"Consumed $name as $body. (Allocated)"))(_ => IO.println(s"Release $name resource"))
						_ <- Resource.make(IO.println(s"  [consumer] Dumped $name body to $tempFile"))(_ => {
							IO.println(s"  [consumer] Releasing temp file $tempFile")
						})
					} yield Right(name -> tempFile.toString)
		},
	)

	def run(args: List[String]): IO[ExitCode] = {
		partsParsed.use { allParts =>
			IO.println(s"[RESULT] All parts hopefully allocated: $allParts").as(ExitCode.Success) <* IO.sleep(1.second)
		}
	}
}

