import com.google.auth.oauth2.GoogleCredentials
import com.google.protobuf.ByteString.Output
import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}
import org.sparkproject.jetty.util.security.Password
import org.apache.log4j.{Level, Logger}

import java.io.File
import java.io.PrintWriter
import java.lang.System.console
import java.time.LocalDateTime
import scala.reflect.internal.Reporter.ERROR
import scala.util.Using.resources

object dbConnector {
  var query: String = null;
  var driver: String = null;
  var url: String = null;
  var login: Config = null;
  var user: String = null
  var password: String = null
  var output: String = null
  var mod: String = null
  var gcs: String = null
  var bigquery: String = null

  def main(args: Array[String]): Unit = {

    setDB("mysql")
    Connector(query, driver, url, user, password, output, "mysql", mod, gcs, bigquery)
    //Starting to postgres
    setDB("postgres")
    Connector(query, driver, url, user, password, output, "postgres", mod, gcs, bigquery)

    def Connector(query: String, driver: String, url: String, user: String, password: String, output: String, db: String, mod: String, gcs: String, bigquery: String) {

      val spark = SparkSession.builder().master("local[*]")
        .appName("dbReader")
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")

      val df = spark.read
        .format("jdbc")
        .option("query", query)
        .option("driver", driver)
        .option("url", url)
        .option("user", user)
        .option("password", password)
        .load()

      val fileName = output + "/" + db + "_" + query.filterNot(_.isWhitespace) + ".json"
      println(fileName)
      df.repartition(1).write.mode(mod).json(fileName)
      val gcsResult = gcs.toLowerCase()
      val bigqueryResult = bigquery.toLowerCase()

      if (gcsResult.equals("yes")) {
        val wg = writeToGcs
        wg.gcs(fileName)
      }
      if (bigqueryResult.equals("yes")) {
        val wg = writeToGcs
        wg.big(fileName)
      }

    }

    def setDB(db: String): Unit = {
      val config = ConfigFactory.load("application.conf").getConfig("com.dbs.sca")
      val dbConfig = config.getConfig(db)

      query = dbConfig.getString("query")
      driver = dbConfig.getString("driver")
      url = dbConfig.getString("url")
      output = dbConfig.getString("output_path")
      mod = dbConfig.getString("mod")
      gcs = dbConfig.getString("gcs")
      bigquery = dbConfig.getString("bigquery")

      val loginPath = dbConfig.getString("login_path")
      val parsedLoginConfig = ConfigFactory.parseFile(new File(loginPath))

      val loginInfo = ConfigFactory.load(parsedLoginConfig).getConfig("com.login.sca")
      login = loginInfo.getConfig(db)
      user = login.getString("username")
      password = login.getString("password")
    }
  }


}
