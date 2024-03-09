package io.dylemma.sandbox

import java.nio.charset.StandardCharsets
import scala.concurrent.duration.DurationInt

import cats.data.EitherT
import cats.effect.std.Supervisor
import cats.effect._
import cats.syntax.all._
import fs2.concurrent.Channel
import fs2.io.file.Files
import fs2.{ Chunk, Pull, Stream }
import org.http4s.multipart.Part
import org.http4s.{ DecodeFailure, DecodeResult, Headers, InvalidMessageBodyFailure, MalformedMessageBodyFailure, ParseResult }

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

	def parseMultipartSupervised[A](
		events: Stream[IO, Event],
		supervisor: Supervisor[IO],
		partConsumer: Part[IO] => Resource[IO, Either[DecodeFailure, A]],
	): EitherT[IO, DecodeFailure, List[A]] = {
		def pullPartStart(
			s: Stream[IO, Event],
		): Pull[IO, Either[DecodeFailure, A], Unit] = s.pull.uncons1.flatMap {
			case Some((ps: PartStart, tail)) =>

				Pull.eval { Channel.synchronous[IO, Chunk[Byte]] }.flatMap { ch =>

					// Create a `Resource` that can consume the Channel's `stream`
					// to produce an output or an error. Make sure that if the
					// Resource doesn't actually consume the whole channel stream
					// for whatever reason (errors, ignoring, or otherwise), that
					// we close the channel once an outcome is reached. Otherwise
					// the producer side of the channel (i.e. the main Pull loop)
					// would hang while trying to consume the stream of events.
					val receiverRes = partConsumer(Part(ps.headers, ch.stream.unchunks))
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
									a => pullPartStart(restOfStream),
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
			.translate(EitherT.liftK[IO, DecodeFailure])
			.flatMap(r => Stream.eval(EitherT.fromEither[IO](r)))
			.compile
			.toList
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
					.useForever,
			)
		} yield promise
	}

	val receiver = (
		MultipartReceiver[IO].textPart("foo")(_.readString),
		MultipartReceiver[IO].auto,
	).tupled

	def decodeMultipart[A](
		partEvents: Stream[IO, Event],
		supervisor: Supervisor[IO],
		receiver: MultipartReceiver[IO, A],
	): DecodeResult[IO, A] = {
		val receiverWrapped = receiver.rejectUnexpectedParts
		parseMultipartSupervised[receiver.Partial](
			partEvents,
			supervisor,
			part => receiverWrapped.decide(part.headers).get.receive(part)
		).flatMap { partials =>
			EitherT.fromEither[IO] { receiver.assemble(partials) }
		}
	}

	def run(args: List[String]): IO[ExitCode] = {
		Supervisor[IO].use { supervisor =>
			parseMultipartSupervised[receiver.Partial](
				exampleEvents.covary[IO].metered(50.millis).evalTap(IO.println),
				supervisor,
				part => {
					val r = receiver.decide(part.headers).getOrElse {
						PartReceiver[IO].reject[receiver.Partial] {
							part.name match {
								case None => InvalidMessageBodyFailure("Unexpected anonymous part")
								case Some(name) => InvalidMessageBodyFailure(s"Unexpected part: '$name'")
							}
						}
					}
					r.receive(part)
				}
			).value.flatMap { res =>
				val assembledResult = res.flatMap { partials =>
					receiver.assemble(partials)
				}

				IO.println(s"[RESULT] All parts hopefully allocated: $assembledResult").as(ExitCode.Success).andWait(1.second)
			}

		} <* IO.println("closed supervisor")
	}
}

