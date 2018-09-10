import Example.{ Friend, Person, Stranger }
import SqlAnnotations.{ id, tableName }
import cats.effect.IO

import scala.collection.mutable
import scala.concurrent.{ Await, Future }
import sangria.execution.Executor
import sangria.macros.derive._
import sangria.schema._
import sangria.schema.ObjectType
import sangria.macros._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

trait GQL {}

object GraphQL extends App {
  implicit val idTransformer = new IdTransformer[Int] {
    override def fromString(str: String): Int =
      str.toInt
  }
  implicit val (url, xa) = PostgresStuff.go()

  sealed trait ContactInfo
  case class Email(name: String, domain: String)                   extends ContactInfo
  case class PhoneNumber(countryCode: Option[String], number: Int) extends ContactInfo
  case class Address(street: String, number: Int)                  extends ContactInfo

  sealed trait Pet
  case class Cat(name: String)           extends Pet
  case class Dog(name: String, age: Int) extends Pet
  @tableName("users") case class User(@id id: Int, name: String, age: Int)

  val userRepo = RepoOps.toRepo[Int, User](RepoOps.gen[User])

  implicit lazy val userType: ObjectType[Repo[Int, User], User] = deriveObjectType[Repo[Int, User], User]()

  val idArg = Argument("id", IntType)

  val queryType: ObjectType[Repo[Int, User], Unit] = ObjectType(
    "query",
    fields[Repo[Int, User], Unit](
      Field("user",
            OptionType(userType),
            arguments = idArg :: Nil,
            resolve = c => c.ctx.findById(c arg idArg).unsafeRunSync())
    )
  )

  val schema = Schema(queryType)

  val query =
    gql"""
          {
            user(id: 1) {
              name,
              age
            }
          }
      """

  val exampleUser = User(1, "Rune", 32)

  val prog = for {
    _     <- userRepo.createTables()
    id    <- userRepo.insert(exampleUser)
    found <- userRepo.findById(id)
    gql <- IO.fromFuture {
            IO(Executor.execute(schema, query, userRepo))
          }
  } yield (found, gql)

  val start         = System.nanoTime() nanos;
  val (friend, gql) = prog.unsafeRunSync()
  val end           = System.nanoTime() nanos;

  println((end - start).toMillis)

  println(friend)
  println(gql)
}
