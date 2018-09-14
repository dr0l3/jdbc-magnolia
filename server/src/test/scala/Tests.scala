import SqlAnnotations.id
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

object Tests extends Properties("RepoProps") {

  case class SimpleCaseClassExample(@id id: Int, string: String, double: Double, anotherInt: Int)

  implicit val (url, xa) = PostgresStuff.go();
  implicit val arbString = Arbitrary[String](Gen.alphaStr)

  implicit val idTransformer = new IdTransformer[Int] {
    override def fromString(str: String): Int = str.toInt
  }

  val caseClassRepo = RepoOps.toRepo(RepoOps.gen[SimpleCaseClassExample])

  implicit val arb = implicitly[Arbitrary[SimpleCaseClassExample]]

  property("Can insert and find simple case classes") = forAll { a: SimpleCaseClassExample =>
    val prog = for {
      _ <- caseClassRepo.createTables()
      inserted <- caseClassRepo.insert(a)
      found <- caseClassRepo.findById(inserted)
    } yield {
      found.isDefined &&
        found.get.copy(id = a.id) == a
    }

    prog.unsafeRunSync()
  }

  sealed trait Trait
  case class FirstImplementation(@id id: String, string: String, double: Double) extends Trait
  case class SecondImplementation(@id id: String, int: Int, double: Double) extends Trait

  implicit val idTransformerString = new IdTransformer[String] {
    override def fromString(str: String): String = str
  }

  val traitRepo = RepoOps.toRepo(RepoOps.gen[Trait])(transactor = xa, idTransformer = idTransformerString)

  implicit val arbTrait = implicitly[Arbitrary[Trait]]

  property("Can insert and find traits") = forAll { a: Trait =>
    val prog = for {
      _ <- traitRepo.createTables()
      id <- traitRepo.insert(a)
      found <- traitRepo.findById(id)
    } yield {
      val equality = (found.get, a) match {
        case (a: FirstImplementation, b: FirstImplementation) => a.copy(id = b.id) == b
        case (a: SecondImplementation, b: SecondImplementation) => a.copy(id = b.id) == b
        case _ => false
      }

      found.isDefined && equality
    }

    prog.unsafeRunSync()
  }

  sealed trait ComplexExample
  case class A(@id id: Int, name: String) extends ComplexExample
  case class Base(@id id: Int, namez: String) extends ComplexExample
  sealed trait C
  case class D(@id string: String, double: Double) extends C
  case class E(@id string: String, age: Int, height: Double) extends C

  implicit val arbComplex = implicitly[Arbitrary[ComplexExample]]
  val complexRepo = RepoOps.toRepo(RepoOps.gen[ComplexExample])(transactor = xa, idTransformer = idTransformer)

  property("Can insert and find complex example") = forAll { complex: ComplexExample =>
    val prog = for {
      _ <- complexRepo.createTables()
      id <- complexRepo.insert(complex)
      found <- complexRepo.findById(id)
    } yield found.isDefined

    prog.unsafeRunSync()
  }

  case class Lists(@id id: Int, string: String, list: List[String], list2: List[SomeCaseClass])
  case class SomeCaseClass(@id string: Int, anotherString: String)
  implicit val listsArb = implicitly[Arbitrary[Lists]]
  val listsRepo = RepoOps.toRepo(RepoOps.gen[Lists])(transactor = xa, idTransformer = idTransformer)

  property("can insert find lists") = forAll { l: Lists =>
    val prog = for {
      _ <- listsRepo.createTables()
      id <- listsRepo.insert(l)
      found <- listsRepo.findById(id)
    } yield {
      found.isDefined &&
        found.get.list.size == l.list.size &&
        found.get.list2.size == l.list2.size
    }

    prog.unsafeRunSync()
  }
}
