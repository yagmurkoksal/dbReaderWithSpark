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

}
