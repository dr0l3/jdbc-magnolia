import Shared.{idTransformerString, xa}
import SqlAnnotations.{fieldName, id, tableName}
import Tests.property
import org.scalacheck.Properties
import cats._
import cats.data._
import cats.effect._
import cats.implicits._
import doobie._
import doobie.implicits._
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres._
import org.scalacheck.Prop.forAll
import scalacheckmagnolia.MagnoliaArbitrary._
import org.scalacheck._

object Shared {
  implicit val (url, xa) = PostgresStuff.goLive();
  implicit val arbString = Arbitrary[String](Gen.alphaStr)

  implicit val idTransformer = new IdTransformer[Int] {
    override def fromString(str: String): Int = str.toInt
  }

  implicit val idTransformerString = new IdTransformer[String] {
    override def fromString(str: String): String = str
  }
}

object Tests extends Properties("RepoProps") {
  import Shared._

  case class SimpleCaseClassExample(@id id: Int, string: String, double: Double, anotherInt: Int, bool: Boolean)

  val caseClassRepo = RepoOps.toRepo(RepoOps.gen[SimpleCaseClassExample])(transactor = xa, idTransformer = idTransformer)

  implicit val arb = implicitly[Arbitrary[SimpleCaseClassExample]]

  property("Can insert and find simple case classes") = forAll { a: SimpleCaseClassExample =>
    val prog = for {
      _        <- caseClassRepo.createTables()
      inserted <- caseClassRepo.insert(a)
      found    <- caseClassRepo.findById(inserted)
    } yield {
      found.isDefined &&
      found.get.copy(id = a.id) == a
    }

    prog.unsafeRunSync()
  }


}

object TraitTest extends Properties("Repo.trait") {
  import Shared._
  sealed trait Trait
  case class FirstImplementation(@id id: String, string: String, double: Double) extends Trait
  case class SecondImplementation(@id id: String, int: Int, double: Double)      extends Trait

  val traitRepo = RepoOps.toRepo(RepoOps.gen[Trait])(transactor = xa, idTransformer = idTransformerString)

  implicit val arbTrait = implicitly[Arbitrary[Trait]]

  property("Can insert and find traits") = forAll { a: Trait =>
    val prog = for {
      _     <- traitRepo.createTables()
      id    <- traitRepo.insert(a)
      found <- traitRepo.findById(id)
    } yield {
      val equality = (found.get, a) match {
        case (a: FirstImplementation, b: FirstImplementation)   => a.copy(id = b.id) == b
        case (a: SecondImplementation, b: SecondImplementation) => a.copy(id = b.id) == b
        case _                                                  => false
      }

      found.isDefined && equality
    }

    prog.unsafeRunSync()
  }
}

object ComplexTests extends Properties("Repo.complex") {
  import Shared._
  sealed trait ComplexExample
  case class A(@id id: Int, name: String)     extends ComplexExample
  case class Base(@id id: Int, namez: String) extends ComplexExample
  sealed trait C
  case class D(@id string: String, double: Double)           extends C
  case class E(@id string: String, age: Int, height: Double) extends C

  implicit val arbComplex = implicitly[Arbitrary[ComplexExample]]
  val complexRepo         = RepoOps.toRepo(RepoOps.gen[ComplexExample])(transactor = xa, idTransformer = idTransformer)

  property("Can insert and find complex example") = forAll { complex: ComplexExample =>
    val prog = for {
      _     <- complexRepo.createTables()
      id    <- complexRepo.insert(complex)
      found <- complexRepo.findById(id)
    } yield found.isDefined

    prog.unsafeRunSync()
  }
}

object ListTest extends Properties("Repo.list") {
  import Shared._
  case class Lists(@id id: Int, string: String, list: List[String], list2: List[SomeCaseClass])
  case class SomeCaseClass(@id string: Int, anotherString: String)
  implicit val listsArb = implicitly[Arbitrary[Lists]]
  val listsRepo         = RepoOps.toRepo(RepoOps.gen[Lists])(transactor = xa, idTransformer = idTransformer)

  property("can insert find lists") = forAll { l: Lists =>
    val prog = for {
      _     <- listsRepo.createTables()
      id    <- listsRepo.insert(l)
      found <- listsRepo.findById(id)
      res <- Fragment.const(
        s"""
           |SELECT schemaname,relname,n_live_tup
           |  FROM pg_stat_user_tables
           |  ORDER BY n_live_tup DESC;
       """.stripMargin).query[(String, String, Int)].to[List].transact(xa)
      _ = println(res)
    } yield {
      found.isDefined &&
        found.get.list.size == l.list.size &&
        found.get.list2.size == l.list2.size
    }

    prog.unsafeRunSync()
  }
}

object AnnotationTests extends Properties("Repo.annotation") {
  implicit val (url, xa) = PostgresStuff.go();
  implicit val arbString = Arbitrary[String](Gen.alphaStr)

  implicit val idTransformer = new IdTransformer[Int] {
    override def fromString(str: String): Int = str.toInt
  }

  @tableName("random_table") case class WithAnnotations(@id @fieldName("random_name") id: Int, @fieldName("another") str: String)
  implicit val annotArb = implicitly[Arbitrary[WithAnnotations]]

  val annoRepo = RepoOps.toRepo(RepoOps.gen[WithAnnotations])(transactor = xa, idTransformer = idTransformer)

  property("Can insert and find things with annotations") = forAll { a: WithAnnotations =>
    val prog = for {
      _ <- annoRepo.createTables()
      id <- annoRepo.insert(a)
      found <- annoRepo.findById(id)
    } yield found.isDefined

    prog.unsafeRunSync()
  }
}
