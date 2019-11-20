package example

import cats._, cats.data._, cats.implicits._
import cats.effect._, cats.effect.implicits._

object Main extends IOApp {

  trait KeyValueStore[F[_], Key, Value] {
    def insert(key: Key, value: Value): F[Key]
    def update(key: Key, value: Value): F[Value]
    def delete(key: Key): F[Value]
    def query(key: Key): F[Option[Value]]
  }

  object KeyValueStore {
    def apply[F[_], K, V](implicit ev: KeyValueStore[F, K, V]) = ev
  }

  type Effect[A] = StateT[Either[Throwable, ?], Map[String, String], A]

  implicit object KeyValueStoreEffect extends KeyValueStore[Effect, String, String] {
    def insert(key: String, value: String): Effect[String] =
      StateT
        .modifyF { s: Map[String, String] =>
          if (s.contains(key)) new Exception("Data exists").asLeft
          else s.updated(key, value).asRight[Throwable]
        }
        .map(_ => key)

    def update(key: String, value: String): Effect[String] =
      StateT
        .modifyF { s: Map[String, String] =>
          if (!s.contains(key)) new Exception("Data not exists").asLeft
          else s.updated(key, value).asRight[Throwable]
        }
        .map(_ => value)

    def delete(key: String): Effect[String] =
      StateT { s: Map[String, String] =>
        if (!s.contains(key)) new Exception("Data not exists").asLeft
        else ((s - key), s(key)).asRight[Throwable]
      }

    def query(key: String): Effect[Option[String]] =
      StateT.inspect(s => s.get(key))
  }

  trait Printable[F[_]] {
    def print(s: String): F[Unit]
    def println(s: String): F[Unit] = print(s + "\n")
  }

  object Printable {
    def apply[F[_]](implicit ev: Printable[F]) = ev
  }

  implicit object PrintableEffect extends Printable[Effect] {
    def print(s: String): Effect[Unit] = scala.Console.out.print(s).pure[Effect]
  }

  def program[F[_]: KeyValueStore[?[_], String, String]: Printable: Monad] = for {
    _ <- KeyValueStore[F, String, String].insert("k1", "A")
    v1 <- KeyValueStore[F, String, String].query("k1")
    _ <- Printable[F].println(v1.toString)
    _ <- Printable[F].println(v1.toString)
  } yield ()

  def run(args: List[String]): IO[ExitCode] = {
    IO(program[Effect].run(Map.empty)).as(ExitCode.Success)
  }
}
