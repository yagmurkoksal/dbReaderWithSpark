
import com.google.cloud.storage.Bucket
import com.google.cloud.storage.BucketInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.StorageOptions
import java.io._

object writeToGcs {

  @throws[Exception]
  def gcs(file: String) {
    val storage = StorageOptions.getDefaultInstance.getService
    val bucketName = "mysql-postgres"
    val bucket = storage.create(BucketInfo.of(bucketName))
    System.out.printf("Bucket %s created.%n", bucket.getName)
  }

  @throws[Exception]
  def big(file: String): Unit = {

  }


}
