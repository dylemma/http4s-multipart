package io.dylemma.sandbox

import cats.Applicative
import cats.effect.Sync
import cats.effect.kernel.Resource
import cats.implicits.toFoldableOps
import fs2._
import fs2.io.file.{ Files, Path }
import org.http4s.headers.`Content-Disposition`
import org.http4s.multipart.Part
import org.http4s.{ EntityBody, Headers, ParseFailure, ParseResult }
import org.typelevel.ci.CIStringSyntax

trait PartReceiver[F[_], A] {
	def receive(part: Part[F]): Resource[F, ParseResult[A]]

	def map[B](f: A => B): PartReceiver[F, B] = part => this.receive(part).map(_.map(f))
}

object PartReceiver {

	def apply[F[_]] = new PartialApply[F]

	class PartialApply[F[_]] {
		def readString(implicit F: RaiseThrowable[F], cmp: Compiler[F, F]): PartReceiver[F, String] =
			part => Resource.eval(part.bodyText.compile.string).map(Right(_))

		def toTempFile(implicit F: Files[F], c: Compiler[F, F]): PartReceiver[F, Path] =
			part => F.tempFile.evalTap { path => part.body.through(F.writeAll(path)).compile.drain }.map(Right(_))

		def ignore: PartReceiver[F, Unit] =
			part => Resource.pure(Right(()))

		def reject[A](err: ParseFailure): PartReceiver[F, A] =
			part => Resource.pure(Left(err))
	}
}

trait MultipartReceiver[F[_], A] {
	type Partial
	def decide(partHeaders: Headers): Option[PartReceiver[F, Partial]]
	def assemble(partials: List[Partial]): ParseResult[A]
}

object MultipartReceiver {
	def apply[F[_]] = new PartialApply[F]

	private def partName(headers: Headers) = headers.get[`Content-Disposition`].flatMap(_.parameters.get(ci"name"))
	private def partFilename(headers: Headers) = headers.get[`Content-Disposition`].flatMap(_.parameters.get(ci"filename"))
	private def fail(message: String) = ParseFailure(message, "")

	sealed trait FieldValue {
		def rawValue[F[_]: Files]: EntityBody[F]
		def foldValue[A](onString: String => A, onFile: (String, Path) => A): A
	}
	object FieldValue {
		case class OfString(value: String) extends FieldValue {
			def rawValue[F[_]: Files] = fs2.text.utf8.encode[F](Stream.emit(value))
			def foldValue[A](onString: String => A, onFile: (String, Path) => A) = onString(value)
		}
		case class OfFile(filename: String, path: Path) extends FieldValue {
			def rawValue[F[_]: Files]: EntityBody[F] = Files[F].readAll(path)
			def foldValue[A](onString: String => A, onFile: (String, Path) => A): A = onFile(filename, path)
		}
	}

	class PartialApply[F[_]] {
		private def R = PartReceiver[F]

		def expectString(name: String)(implicit F: RaiseThrowable[F], c: Compiler[F, F]): MultipartReceiver[F, String] = new MultipartReceiver[F, String] {
			type Partial = String
			def decide(partHeaders: Headers): Option[PartReceiver[F, Partial]] = (partName(partHeaders), partFilename(partHeaders)) match {
				case (Some(`name`), None) => Some(R.readString)
				case (Some(`name`), Some(_)) => Some(R.reject(fail(s"Unexpected file content in '$name' part")))
				case _ => None
			}
			def assemble(partials: List[String]): ParseResult[String] = partials match {
				case Nil => Left(fail(s"Missing expected part: '$name'"))
				case one :: Nil => Right(one)
				case _ => Left(fail(s"Unexpected multiple parts found with name '$name'"))
			}
		}

		def expectFile(name: String)(implicit F: Files[F], c: Compiler[F, F]): MultipartReceiver[F, (String, Path)] = new MultipartReceiver[F, (String, Path)] {
			type Partial = (String, Path)
			def decide(partHeaders: Headers): Option[PartReceiver[F, Partial]] = (partName(partHeaders), partFilename(partHeaders)) match {
				case (Some(`name`), Some(filename)) => Some(R.toTempFile.map(filename -> _))
				case (Some(`name`), None) => Some(R.reject(fail(s"Expected file content in '$name' part")))
				case _ => None
			}
			def assemble(partials: List[Partial]): ParseResult[(String, Path)] = partials match {
				case Nil => Left(fail(s"Missing expected part: '$name'"))
				case one :: Nil => Right(one)
				case _ => Left(fail(s"Unexpected multiple parts found with name '$name'"))
			}
		}

		def auto(implicit F: Sync[F], files: Files[F]): MultipartReceiver[F, Map[String, FieldValue]] = new MultipartReceiver[F, Map[String, FieldValue]] {
			type Partial = (String, FieldValue)
			def decide(partHeaders: Headers): Option[PartReceiver[F, Partial]] = (partName(partHeaders), partFilename(partHeaders)) match {
				case (Some(name), Some(filename)) => Some(R.toTempFile.map { path => name -> FieldValue.OfFile(filename, path) })
				case (Some(name), None) => Some(R.readString.map { str => name -> FieldValue.OfString(str) })
				case _ => None
			}
			def assemble(partials: List[Partial]): ParseResult[Map[String, FieldValue]] = Right(partials.toMap)
		}
	}

	implicit def multipartReceiverApplicative[F[_]]: Applicative[MultipartReceiver[F, *]] = new MultipartReceiverApplicative[F]

	class MultipartReceiverApplicative[F[_]] extends Applicative[MultipartReceiver[F, *]] {
		def pure[A](x: A): MultipartReceiver[F, A] = new MultipartReceiver[F, A] {
			type Partial = Unit
			def decide(partHeaders: Headers): Option[PartReceiver[F, Partial]] = None
			def assemble(partials: List[Partial]): ParseResult[A] = Right(x)
		}
		def ap[A, B](ff: MultipartReceiver[F, A => B])(fa: MultipartReceiver[F, A]): MultipartReceiver[F, B] = new MultipartReceiver[F, B] {
			type Partial = Either[ff.Partial, fa.Partial]
			def decide(partHeaders: Headers): Option[PartReceiver[F, Either[ff.Partial, fa.Partial]]] = {
				// the order that the `orElse` operands appear decides the precedence in cases like `(receiver1, receiver2).mapN`
				// where both receivers could decide to consume the Part; only one is allowed, so we have to choose a winner.
				ff.decide(partHeaders).map(_.map(Left(_): Partial)) orElse fa.decide(partHeaders).map(_.map(Right(_): Partial))
			}
			def assemble(partials: List[Either[ff.Partial, fa.Partial]]): ParseResult[B] = {
				val (ffs, fas) = partials.partitionEither(identity)
				for {
					f <- ff.assemble(ffs)
					a <- fa.assemble(fas)
				} yield f(a)
			}
		}
	}
}