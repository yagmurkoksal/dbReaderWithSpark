
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalTime;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;

public class writeToGcsAndBigQuery {
    String projectId = "capable-epigram-385107";
    String cred = "src/resources/capable-epigram-385107-6b96dfb8ac7b.json";

    public void write(String fileName, String filePath, String db, String query) throws IOException {
        String bucketName = "mysqlandpostgres";
        LocalTime ltime = LocalTime.now();
        String objectName = db + "/" + ltime + fileName;

        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).setCredentials(GoogleCredentials.fromStream(new
                FileInputStream(cred))).build().getService();
        BlobId blobId = BlobId.of(bucketName, objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

        File folder = new File(filePath);
        File[] listOfFiles = folder.listFiles();

        for (int i = 0; i < listOfFiles.length; i++) {
            String ext = FilenameUtils.getExtension(listOfFiles[i].getName());

            if (listOfFiles[i].isFile() && ext.equals("json")) {
                storage.createFrom(blobInfo, Paths.get(filePath + "/" + listOfFiles[i].getName()));
                //Writing to gcs ended, writing to big query starts
                String datasetName = db;
                String tableName = query;
                String sourceUri = "gs://" + bucketName + "/" + objectName;
                loadJsonFromGCS(datasetName, tableName, sourceUri);
            }
        }
    }

    public void loadJsonFromGCS(
            String datasetName, String tableName, String sourceUri) {
        try {
            BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId(projectId)
                    .setCredentials(
                            ServiceAccountCredentials.fromStream(new FileInputStream(cred))
                    ).build().getService();

            TableId tableId = TableId.of(datasetName, tableName);
            LoadJobConfiguration loadConfig =
                    LoadJobConfiguration.newBuilder(tableId, sourceUri)
                            .setFormatOptions(FormatOptions.json()).setAutodetect(Boolean.TRUE)
                            .build();

            // Load data from a GCS JSON file into the table
            Job job = bigquery.create(JobInfo.of(loadConfig));
            // Blocks until this load table job completes its execution, either failing or succeeding.
            job = job.waitFor();
            if (job.isDone()) {
                System.out.println("Json from GCS successfully loaded in a table");
            } else {
                System.out.println(
                        "BigQuery was unable to load into the table due to an error:"
                                + job.getStatus().getError());
            }
        } catch (BigQueryException | InterruptedException e) {
            System.out.println("Column not added during load append \n" + e.toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}