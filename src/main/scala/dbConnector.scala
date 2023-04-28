import com.google.protobuf.ByteString.Output
import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}
import org.sparkproject.jetty.util.security.Password

import java.io.{File, PrintWriter, StringWriter}
import java.time.LocalDateTime

object dbConnector {
  var query: String = null;
  var driver: String = null;
  var url: String = null;
  var login:Config=null;
  var user: String =null
  var password:String=null
  var output:String=null
  val errorOutput = new StringWriter

  def main(args: Array[String]): Unit = {

    setDB("mysql")
    Connector(query, driver, url, user,password,output,"mysql")
    //Starting to postgres
    setDB("postgres")
    Connector(query, driver, url, user,password,output,"postgres")


    def Connector(query: String, driver: String, url: String, user: String, password: String,output: String,db:String) {

      val spark = SparkSession.builder().master("local[*]")
        .appName("dbReader")
        .getOrCreate()

      try{
      val df = spark.read
        .format("jdbc")
        .option("query", query)
        .option("driver", driver)
        .option("url", url)
        .option("user", user)
        .option("password", password)
        .load()
      val tim=LocalDateTime.now()
     df.repartition(1).write.json(output+"/"+db+tim+".json")
    }catch {
        case d: Throwable => d.printStackTrace(new PrintWriter(errorOutput))
          new PrintWriter(s"src/main/errors/error"+LocalDateTime.now()+".txt") //Saves error message to this location
          {
            write(errorOutput.toString);
            close
          }}}

    def setDB(db: String): Unit = {
      val config = ConfigFactory.load("application.conf").getConfig("com.dbs.sca")
      val dbConfig = config.getConfig(db)

      query = dbConfig.getString("query")
      driver = dbConfig.getString("driver")
      url = dbConfig.getString("url")
      output=dbConfig.getString("output_path")

      val loginPath = dbConfig.getString("login_path")
      val parsedLoginConfig = ConfigFactory.parseFile(new File(loginPath))

      val loginInfo = ConfigFactory.load(parsedLoginConfig).getConfig("com.login.sca")
       login =loginInfo.getConfig(db)
       user= login.getString("username")
       password=login.getString("password")
    }
  }


}
