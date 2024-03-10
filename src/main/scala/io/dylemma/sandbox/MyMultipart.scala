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

	/** Pipe that receives PartStart / PartChunk / PartEnd events,
	  * extracting the chunk data between each PartStart / PartEnd
	  * pair and feeding it through a new `partDecoder` for each part.
	  * Emits the results from each `partDecoder`.
	  *
	  * Resources allocated by the returned Pipe are supervised by
	  * the given `supervisor`, such that when the `supervisor` closes,
	  * the allocated resources also close.
	  *
	  * The `partDecoder` may or may not actually *consume* the part body.
	  * The underlying parser will wait at a `PartEnd` event until the
	  * consumer finishes, i.e. the point when its Resource allocates a value
	  * or raises an error. If the Resource never allocates, the Pipe will
	  * hang forever.
	  *
	  * @param supervisor  A Supervisor used to restrict the lifetimes
	  *                    of resources allocated by this Pipe
	  * @param partDecoder A function that consumes the body of a part.
	  *                    Will be called once for each `PartStart` in
	  *                    the incoming stream.
	  * @tparam A Value type returned for each successfully-parsed Part
	  * @return A pipe that transforms Part events into corresponding
	  *         decoded values.
	  */
	def decodePartsPipe[F[_], A](
		supervisor: Supervisor[F],
		partDecoder: Part[F] => DecodeResult[Resource[F, *], A],
	)(implicit F: Concurrent[F]): Pipe[F, Event, Either[DecodeFailure, A]] = {

		def pullPartStart(
			s: Stream[F, Event],
		): Pull[F, Either[DecodeFailure, A], Unit] = s.pull.uncons1.flatMap {
			case Some((ps: PartStart, tail)) =>

				for {
					// Created a shared channel that allows us to represent the `partConsumer`
					// logic as a `Stream` consumer rather than some kind of "scan" operation.
					// As new `PartChunk` events are pulled, they will be sent to the channel,
					// and concurrently the `partConsumer` will consume the channel's `.stream`.
					channel <- Pull.eval { Channel.synchronous[F, Chunk[Byte]] }

					// Since we'll run the `partConsumer` concurrently while pulling chunk events
					// from the input stream, we use a `Deferred` to allow us to block on the
					// consumer's completion. This also lets us un-block the event-pull during its
					// attempts to `send` to the channel, in case the consumer completed without
					// consuming the entire stream.
					resultPromise <- Pull.eval { Deferred[F, Either[Throwable, A]] }

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
							val keepPulling = F.pure(true)
							val stopPulling = F.pure(false)
							F.race(channel.send(chunk), resultPromise.get).flatMap {
								case Left(_) => keepPulling // send completed normally; don't care if it was closed or not
								case Right(Right(a)) => keepPulling // send may have blocked, but the receiver already has a result
								case Right(Left(err)) => stopPulling // receiver raised an error, so abort the pull
							}
						})
							// when this part of the Pull completes, make sure to close the channel
							// so that the `receiver` Stream sees an EOF signal.
							.handleErrorWith { err =>
								Pull.eval(channel.close) >> Pull.raiseError[F](err)
							}
							.productL { Pull.eval(channel.close) }
					}

					// Once we've reached the end of the current part, wait until the consumer
					// finishes and sends its result (or error) to the promise.
					partResult <- Pull.eval(resultPromise.get)

					// Output the partResult, continuing the Pull if it was not an error
					_ <- partResult match {
						case Right(a) => Pull.output1(Right(a)) >> pullPartStart(restOfStream)
						case Left(e: DecodeFailure) => Pull.output1(Left(e)).covary[F] >> Pull.done
						case Left(e) => Pull.raiseError[F](e)
					}
				} yield ()

			case None =>
				Pull.done

			case Some((PartEnd, _)) =>
				Pull.raiseError[F](new IllegalStateException("unexpected PartEnd"))

			case Some((PartChunk(_), _)) =>
				Pull.raiseError[F](new IllegalStateException("unexpected PartChunk"))
		}

		def pullUntilPartEnd(s: Stream[F, Event], pushChunk: Chunk[Byte] => F[Boolean]): Pull[F, Nothing, Stream[F, Event]] = s.pull.uncons1.flatMap {
			case Some((PartEnd, tail)) =>
				Pull.pure(tail)
			case Some((PartChunk(chunk), tail)) =>
				Pull.eval(pushChunk(chunk)).flatMap { keepPulling =>
					if (keepPulling) pullUntilPartEnd(tail, pushChunk)
					else Pull.pure(Stream.empty)
				}
			case Some((PartStart(_), _)) | None =>
				Pull.raiseError[F](new IllegalStateException("Missing PartEnd"))
		}

		events => pullPartStart(events).stream
	}

	def decodeMultipartSupervised[F[_]: Concurrent, A](
		events: Stream[F, Event],
		supervisor: Supervisor[F],
		partConsumer: Part[F] => EitherT[Resource[F, *], DecodeFailure, A],
	): DecodeResult[F, List[A]] = {
		events
			.through { decodePartsPipe(supervisor, partConsumer) }
			.translate(EitherT.liftK[F, DecodeFailure])
			.flatMap(r => Stream.eval(EitherT.fromEither[F](r)))
			.compile
			.toList
	}

	def decodeMultipart[F[_]: Concurrent, A](
		partEvents: Stream[F, Event],
		supervisor: Supervisor[F],
		receiver: MultipartReceiver[F, A],
	): DecodeResult[F, A] = {
		val receiverWrapped = receiver.rejectUnexpectedParts
		decodeMultipartSupervised[F, receiverWrapped.Partial](
			partEvents,
			supervisor,
			part => EitherT(receiverWrapped.decide(part.headers).get.receive(part)),
		).flatMap { partials =>
			EitherT.fromEither[F] { receiverWrapped.assemble(partials) }
		}
	}

}

