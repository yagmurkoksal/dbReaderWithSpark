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
  var login:Config=null;
  var user: String =null
  var password:String=null
  var output:String=null
  var mod:String=null

  def main(args: Array[String]): Unit = {

    setDB("mysql")
    Connector(query, driver, url, user,password,output,"mysql",mod)
    //Starting to postgres
    setDB("postgres")
    Connector(query, driver, url, user,password,output,"postgres",mod)

    def Connector(query: String, driver: String, url: String, user: String, password: String,output: String,db:String,mod:String) {

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
      val tim=LocalDateTime.now()
     //df.repartition(1).write.json(output+"/"+db+tim+".json")

     df.write.mode(mod).json("gs:/mysqlandpostgres/")

    }

    def setDB(db: String): Unit = {
      val config = ConfigFactory.load("application.conf").getConfig("com.dbs.sca")
      val dbConfig = config.getConfig(db)

      query = dbConfig.getString("query")
      driver = dbConfig.getString("driver")
      url = dbConfig.getString("url")
      output=dbConfig.getString("output_path")
      mod=dbConfig.getString("mod")

      val loginPath = dbConfig.getString("login_path")
      val parsedLoginConfig = ConfigFactory.parseFile(new File(loginPath))

      val loginInfo = ConfigFactory.load(parsedLoginConfig).getConfig("com.login.sca")
       login =loginInfo.getConfig(db)
       user= login.getString("username")
       password=login.getString("password")
    }
  }


}
