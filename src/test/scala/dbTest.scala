import com.google.cloud.bigquery.BigQueryException
import org.scalatest.funsuite.AnyFunSuite

class dbTest extends AnyFunSuite {

  test("setdb should fail with nonexistentdb") {
    val e: NullPointerException = intercept[NullPointerException] {
      val s = dbConnector.setDB("non existent db")
    }
    assert(e.isInstanceOf[NullPointerException])
  }
  test("Connector should fail with null parameters") {
    val e: NullPointerException = intercept[NullPointerException] {
      val s = dbConnector.Connector(null, null, null, null, null, null, null, null, null, null)
    }
    assert(e.isInstanceOf[NullPointerException])
  }
  test("Writer should fail with inappropriate file name") {
    val e: BigQueryException = intercept[BigQueryException] {
      val wgb = new writeToGcsAndBigQuery
      val s = wgb.write("bad5263':Fİlena,Me", "src/main/output/mysql_selectcustomerNamefromcustomers.json", "mysql", "select * from customers")
    }
    assert(e.isInstanceOf[BigQueryException])
  }


}
