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

  @tableName("users") case class User(@id id: Int, name: String, age: Int)

  implicit val userRepo = RepoOps.toRepo[Int, User](RepoOps.gen[User])


  class Mutation(repo: Repo[Int, User]) {
    @GraphQLField
    def addUser(name: String, age: Int): User = {
      val user = User(1, name, age)
      val id = repo.insert(user).unsafeRunSync()
      user.copy(id = id)
    }
  }

  case class RepoContext(mutation: Mutation, repo: Repo[Int, User])
  implicit val userType = deriveObjectType[RepoContext, User]()
  val mutationType = deriveContextObjectType[RepoContext, Mutation, Unit](_.mutation)

  val idArg = Argument("id", IntType)

  val queryType = ObjectType(
    "query",
    fields[RepoContext, Unit](
      Field("user", OptionType(userType), arguments = idArg :: Nil, resolve = c => c.ctx.repo.findById(c arg idArg).unsafeRunSync())
    )
  )

  val schema = Schema(queryType, Some(mutationType))

  val query =
    gql"""
          {
            user(id: 1) {
              name,
              age
            }
          }
      """

  val mutQuery =
    gql"""
         mutation {
          addUser( name: "Rune",age : 31) {
            id
          }
         }
       """

  val exampleUser = User(1, "Rune", 32)

  val repoContext = RepoContext(new Mutation(userRepo), userRepo)

  val prog = for {
    _     <- userRepo.createTables()
    id    <- userRepo.insert(exampleUser)
    found <- userRepo.findById(id)
    gql <- IO.fromFuture {
            IO(Executor.execute(schema, query, repoContext))
          }
    gql2 <- IO.fromFuture {
      IO(Executor.execute(schema, mutQuery, repoContext))
    }
    after <- userRepo.findById(2)
  } yield (found, gql, gql2, after)

  val start         = System.nanoTime() nanos;
  val (friend, gql, gql2, after) = prog.unsafeRunSync()
  val end           = System.nanoTime() nanos;

  println((end - start).toMillis)

  println(friend)
  println(gql)
  println(gql2)
  println(after)
}
