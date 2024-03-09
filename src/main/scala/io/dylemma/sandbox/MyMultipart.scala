package io.dylemma.sandbox

import cats.data.EitherT
import cats.effect._
import cats.effect.std.Supervisor
import cats.syntax.all._
import fs2.concurrent.Channel
import fs2.{ Chunk, Pipe, Pull, Stream }
import org.http4s.multipart.Part
import org.http4s.{ DecodeFailure, DecodeResult, Headers }

object MyMultipart {
	sealed trait Event
	case class PartStart(headers: Headers) extends Event
	case class PartChunk(chunk: Chunk[Byte]) extends Event
	case object PartEnd extends Event

	def decodeMultipartEvents[A](
		supervisor: Supervisor[IO],
		partDecoder: Part[IO] => DecodeResult[Resource[IO, *], A],
	): Pipe[IO, Event, Either[DecodeFailure, A]] = {

		def pullPartStart(
			s: Stream[IO, Event],
		): Pull[IO, Either[DecodeFailure, A], Unit] = s.pull.uncons1.flatMap {
			case Some((ps: PartStart, tail)) =>

				for {
					// Created a shared channel that allows us to represent the `partConsumer`
					// logic as a `Stream` consumer rather than some kind of "scan" operation.
					// As new `PartChunk` events are pulled, they will be sent to the channel,
					// and concurrently the `partConsumer` will consume the channel's `.stream`.
					channel <- Pull.eval { Channel.synchronous[IO, Chunk[Byte]] }

					// Since we'll run the `partConsumer` concurrently while pulling chunk events
					// from the input stream, we use a `Deferred` to allow us to block on the
					// consumer's completion. This also lets us un-block the event-pull during its
					// attempts to `send` to the channel, in case the consumer completed without
					// consuming the entire stream.
					resultPromise <- Pull.eval { Deferred[IO, Either[Throwable, A]] }

					// Start the "receiver" fiber to run in the background, sending
					// its result to the `resultPromise` when it becomes available
					_ <- Pull.eval(supervisor.supervise[Nothing] {
						partDecoder(Part(ps.headers, channel.stream.unchunks))
							.value
							.attempt
							.evalTap { r => resultPromise.complete(r.flatten) *> channel.close }
							// tries to allocate the resource but never close it, but since this
							// will be started by the supervisor, when the supervisor closes, it
							// will cancel the usage, allowing the resource to release
							.useForever
					})

					// Continue pulling Chunks for the current Part, feeding them to the receiver
					// via the shared Channel. Make sure the channel push operation doesn't block,
					// by racing its `send` effect with `resultPromise.get`, so that if the receiver
					// decides to abort early, we can stop trying to push to the channel. If the
					// receiver raises an error, stop pulling completely
					restOfStream <- {
						pullUntilPartEnd(tail, chunk => {
							val keepPulling = IO.pure(true)
							val stopPulling = IO.pure(false)
							IO.race(channel.send(chunk), resultPromise.get).flatMap {
								case Left(_) => keepPulling // send completed normally; don't care if it was closed or not
								case Right(Right(a)) => keepPulling // send may have blocked, but the receiver already has a result
								case Right(Left(err)) => stopPulling // receiver raised an error, so abort the pull
							}
						})
							// when this part of the Pull completes, make sure to close the channel
							// so that the `receiver` Stream sees an EOF signal.
							.handleErrorWith { err =>
								Pull.eval(channel.close) >> Pull.raiseError[IO](err)
							}
							.productL { Pull.eval(channel.close) }
					}

					// Once we've reached the end of the current part, wait until the consumer
					// finishes and sends its result (or error) to the promise.
					partResult <- Pull.eval(resultPromise.get)

					// Output the partResult, continuing the Pull if it was not an error
					_ <- partResult match {
						case Right(a) => Pull.output1(Right(a)) >> pullPartStart(restOfStream)
						case Left(e: DecodeFailure) => Pull.output1(Left(e)).covary[IO] >> Pull.done
						case Left(e) => Pull.raiseError[IO](e)
					}
				} yield ()

			case None =>
				Pull.done

			case Some((PartEnd, _)) =>
				Pull.raiseError[IO](new IllegalStateException("unexpected PartEnd"))

			case Some((PartChunk(_), _)) =>
				Pull.raiseError[IO](new IllegalStateException("unexpected PartChunk"))
		}

		def pullUntilPartEnd(s: Stream[IO, Event], pushChunk: Chunk[Byte] => IO[Boolean]): Pull[IO, Nothing, Stream[IO, Event]] = s.pull.uncons1.flatMap {
			case Some((PartEnd, tail)) =>
				Pull.pure(tail)
			case Some((PartChunk(chunk), tail)) =>
				Pull.eval(pushChunk(chunk)).flatMap { keepPulling =>
					if (keepPulling) pullUntilPartEnd(tail, pushChunk)
					else Pull.pure(Stream.empty)
				}
			case Some((PartStart(_), _)) | None =>
				Pull.raiseError[IO](new IllegalStateException("Missing PartEnd"))
		}

		events => pullPartStart(events).stream
	}

	def parseMultipartSupervised[A](
		events: Stream[IO, Event],
		supervisor: Supervisor[IO],
		partConsumer: Part[IO] => EitherT[Resource[IO, *], DecodeFailure, A],
	): DecodeResult[IO, List[A]] = {
		decodeMultipartEvents(supervisor, partConsumer)(events)
			.translate(EitherT.liftK[IO, DecodeFailure])
			.flatMap(r => Stream.eval(EitherT.fromEither[IO](r)))
			.compile
			.toList
	}



	def decodeMultipart[A](
		partEvents: Stream[IO, Event],
		supervisor: Supervisor[IO],
		receiver: MultipartReceiver[IO, A],
	): DecodeResult[IO, A] = {
		val receiverWrapped = receiver.rejectUnexpectedParts
		parseMultipartSupervised[receiver.Partial](
			partEvents,
			supervisor,
			part => EitherT(receiverWrapped.decide(part.headers).get.receive(part)),
		).flatMap { partials =>
			EitherT.fromEither[IO] { receiver.assemble(partials) }
		}
	}

}

