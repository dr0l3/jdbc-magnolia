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
import org.scalacheck.ScalacheckShapeless._

//object Tests extends Properties("Meh"){
//  val creator = TableCreator.gen[Cabin]
//  val saver = Inserter.gen[Cabin]
//  val updater = EasyUpdater.gen[Cabin]
//  val finder = Finder.gen[Cabin]
//
//  implicit val (url , xa) = PostgresStuff.go()
//
//  property("blah") = forAll { (a: Cabin) =>
//    val prog = for {
//      _ <- creator.createTable().right.get._3
//    inserted <- saver.save(a).right.get
//    found <- finder.findById(inserted)
//    changed = found.map(f => f.copy(temperature = f.temperature + 1.1))
//    saved <- updater.update(changed.get).right.get
//    foundAgain <- finder.findById(inserted)
//    } yield {
//      found.isDefined &&
//      foundAgain.isDefined
//    }
//
//    prog.unsafeRunSync()
//  }
//}
