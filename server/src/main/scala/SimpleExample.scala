import java.sql.{ResultSet, Statement}
import java.util.Properties

import cats.data.Chain
import cats.effect.{ExitCode, IO, IOApp}
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import fs2._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.util.Random
import SqlUtils._

object SimpleExample extends IOApp {

  override def run(args: List[String]): IO[ExitCode] = {
    val (url, _) = PostgresStuff.go()
    val config = new HikariConfig(
      "/home/drole/projects/magnolia-testing/server/src/main/resources/application.properties"
    )
    config.setJdbcUrl(url)
    val datasource = new HikariDataSource(config)
    val create     = s"create table if not exists hello_world (id uuid primary key, age INTEGER, name TEXT)"
    val insertSimple = "insert into hello_world (id, name, age) values (?, ?, ?)"
    val select = s"select id,name,age from hello_world"

    val examples2 = (1 to 10000)
      .map{ id =>
        (java.util.UUID.randomUUID(),Random.alphanumeric.take(10), Random.nextInt(100))
      }
    val sql        = s"""
                 |SELECT schemaname,relname,n_live_tup
                 |  FROM pg_stat_user_tables
                 |  ORDER BY n_live_tup DESC;
                 |  """.stripMargin

    val fields = List("schemaname", "relname", "n_live_tup")
    val start  = System.nanoTime() nanos;
    for {
      connection <- IO { datasource.getConnection }
      pStmt      <- IO { connection.prepareStatement(sql) }
      meh        <- IO { connection.createStatement() }
      _          <- IO { meh.execute(create) }
      resultSet  <- IO { pStmt.executeQuery() }
      metadata <- IO {
                   resultSet.getMetaData
                 }
      _ <- IO(println(metadata.getColumnCount))
      _ <- IO(println(metadata.getColumnName(1)))
      res <- extractFieldsImperative(fields, resultSet)
      end = System.nanoTime() nanos;
      start2 = System.nanoTime() nanos;
      stm2 <- IO {
        connection.prepareStatement(insertSimple)
      }
      zomg <- IO {
        examples2.foreach { case (id, name, age) =>
          stm2.setObject(1, id)
          stm2.setObject(2, name.mkString)
          stm2.setObject(3, age)
          stm2.addBatch()
        }
        stm2.executeBatch()
      }
      end2 = System.nanoTime() nanos;
      _ <- IO{
        println(zomg.toList)
        println(s"Handlingtime: ${(end2-start2).toMillis}")
      }
      start3 = System.nanoTime() nanos;
      omgStmt <- IO {
        connection.prepareStatement(select)
      }
      omfgResult <- IO{omgStmt.executeQuery()}
      result <- extractFieldsImperative(List("id", "name", "age"), omfgResult)
      end3 = System.nanoTime() nanos;
      _ <- IO {
        println(s"Size of resuls: ${result.size}")
        println(s"fetch handling time: ${(end3- start3).toMillis}")
      }
      simpleSelect <- IO {connection.createStatement().executeQuery(s"select * from hello_world where id = '${examples2(5000)._1.toString}'")}
      mmuuuh <- extractFieldsImperative(List("id", "name", "age"), simpleSelect)
      _ <- IO {println(mmuuuh)}
      _   <- IO { println(res) }
      _   <- IO { println(s"Handlingtime: ${(end - start).toMillis}") }
    } yield ExitCode.Success
  }
}
