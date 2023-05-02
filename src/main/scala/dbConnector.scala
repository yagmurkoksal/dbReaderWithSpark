import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

import java.io.{File, FileNotFoundException, IOException}
import scala.util.Failure

object dbConnector {
  var query: String = null;
  var driver: String = null;
  var url: String = null;
  var login: Config = null;
  var user: String = null
  var password: String = null
  var output: String = null
  var mod: String = null
  var gcsBigquery: String = null
  var bigquery: String = null

  def main(args: Array[String]): Unit = {

    setDB("mysql")
    Connector(query, driver, url, user, password, output, "mysql", mod, gcsBigquery, bigquery)
    //Starting to postgres
    setDB("postgres")
    Connector(query, driver, url, user, password, output, "postgres", mod, gcsBigquery, bigquery)
  }

  def Connector(query: String, driver: String, url: String, user: String, password: String, output: String, db: String, mod: String, gcsBigquery: String, bigquery: String) {

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

    val fileName = db + "_" + query.filterNot(_.isWhitespace) + ".json"
    val filePath = output + fileName
    df.repartition(1).write.mode(mod).json(filePath)
    val gcsBigqueryResult = gcsBigquery.toLowerCase()

    if (gcsBigqueryResult.equals("True")) {
      val wgb = new writeToGcsAndBigQuery()
      wgb.write(fileName, filePath, db, query)
    }

  }

  def setDB(db: String): Unit = {
    val config = ConfigFactory.load("application.conf").getConfig("com.dbs.sca")
    var dbConfig: Config = null
    try {
      dbConfig = config.getConfig(db)
    }
    catch {
      case _: Throwable => println("Error! Non existing db")
    }

    query = dbConfig.getString("query")
    driver = dbConfig.getString("driver")
    url = dbConfig.getString("url")
    output = dbConfig.getString("output_path")
    mod = dbConfig.getString("mod")
    gcsBigquery = dbConfig.getString("gcsBigquery")

    val loginPath = dbConfig.getString("login_path")
    var parsedLoginConfig: Config = null
    try {
      parsedLoginConfig = ConfigFactory.parseFile(new File(loginPath))
    } catch {
      case e: FileNotFoundException => {
        println(s"File $loginPath not found")
        Failure(e)
      }
      case unknown: Exception => {
        println(s"Unknown exception: $unknown")
        Failure(unknown)
      }
    }

    val loginInfo = ConfigFactory.load(parsedLoginConfig).getConfig("com.login.sca")
    login = loginInfo.getConfig(db)
    user = login.getString("username")
    password = login.getString("password")
  }


}
