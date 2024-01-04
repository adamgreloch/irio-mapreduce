package pl.edu.mimuw.mapreduce.storage.googleapi;

import com.google.cloud.storage.*;
import pl.edu.mimuw.mapreduce.storage.util.UploadObject;

import static java.nio.charset.StandardCharsets.UTF_8;

public class GoogleStorageAPI {
    public static void main(String[] args) {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        // Create a bucket
        String bucketName = "my_unique_bucket"; // Change this to something unique
        Bucket bucket = storage.create(BucketInfo.of(bucketName));

// Upload a blob to the newly created bucket
        BlobId blobId = BlobId.of(bucketName, "my_blob_name");
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
        Blob blob = storage.create(blobInfo, "a simple blob".getBytes(UTF_8));

        // also could be done usind UploadObject
        UploadObject uploadObject = new UploadObject();
        String projectId = "my-project-id";
        try {
            uploadObject.uploadObject(projectId, bucketName, "my_blob_name", "file1.txt");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
