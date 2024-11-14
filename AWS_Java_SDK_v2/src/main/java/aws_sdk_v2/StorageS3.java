package aws_sdk_v2;

import org.apache.commons.io.IOUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.util.function.Predicate.not;

/**
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html
 *
 * TODO all methods weren't checked, only main method
 */
public class StorageS3 {
    public static final String CHECKSUM_FIELD = "content-md5";

    private final S3Client s3Client;
    private final String bucket;

    public StorageS3(S3Client s3Client, String bucket) {
        this.s3Client = s3Client;
        this.bucket = bucket;
    }

    public void save(String name, byte[] body, Optional<String> hash) {
        final Map<String, String> metadata = new HashMap<>();
        hash.ifPresent(h -> {
            metadata.put("x-amz-meta-" + CHECKSUM_FIELD, h);
        });
        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(name)
                .contentLength((long) body.length)
                .contentType("text/plain")
                .metadata(metadata)
                .build();
        s3Client.putObject(putObjectRequest, RequestBody.fromBytes(body));
    }

    // //

    private String get(String name) {
        try (ResponseInputStream<GetObjectResponse> s3Object = getInputStream(name)) {
            return IOUtils.toString(s3Object);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public ResponseInputStream<GetObjectResponse> getInputStream(String name) {
        return s3Client.getObject(getObjectRequest(name));
    }

    public void getToFile(String name, File destination) {
        destination.getParentFile().mkdirs();
        try (FileOutputStream out = new FileOutputStream(destination)) {
            IOUtils.copy(s3Client.getObject(getObjectRequest(name)), out);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Optional<String> getHash(String name) {
        try {
            HeadObjectRequest h = HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(name)
                    .build();
            final HeadObjectResponse resp = s3Client.headObject(h);

            return Optional.ofNullable(resp.metadata().get(CHECKSUM_FIELD))
                    .filter(not(String::isBlank));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    public boolean exists(String key) {
        return !s3Client.listObjects(listObjectsRequest(key)).contents().isEmpty();
    }

    private GetObjectRequest getObjectRequest(String name) {
        return GetObjectRequest.builder().bucket(bucket).key(name).build();
    }

    private ListObjectsRequest listObjectsRequest(String name) {
        return ListObjectsRequest.builder().bucket(bucket).prefix(name).build();
    }

    // //

    S3TransferManager transferManager;

    public void setTransferManager() {
        final DefaultCredentialsProvider credentialsProvider = DefaultCredentialsProvider.create();

        transferManager = S3TransferManager.builder().s3Client(
                S3AsyncClient.crtBuilder()
                        .region(Region.US_EAST_1)
                        .credentialsProvider(credentialsProvider)
                        .maxConcurrency(5)
                        .minimumPartSizeInBytes((long) (5 * 1024 * 1024))
                        .build()
        ).build();


    }

    public void getToFile(String name, String bucket, final File target) throws ExecutionException, InterruptedException {
        transferManager
                .downloadFile(
                        DownloadFileRequest.builder()
                                .getObjectRequest(GetObjectRequest.builder()
                                        .bucket(bucket)
                                        .key(name)
                                        .build())
                                .destination(target.toPath())
                                .build())
                .completionFuture().get();
    }

    public static String readFromAws(AwsCredentialsProvider credentialsProvider) {
        return readFromAws(credentialsProvider, "legacy-report-to-db/stage");
    }

    public static String readFromAws(AwsCredentialsProvider credentialsProvider, String secretName) {
        String region = System.getenv("AWS_REGION");
        region = region != null ? region : "eu-west-1";

        final SecretsManagerClient client = SecretsManagerClient.builder()
                .credentialsProvider(credentialsProvider)
                .region(Region.of(region))
                .build();
        GetSecretValueRequest r = GetSecretValueRequest.builder()
                .secretId(secretName)
                .build();
        String secret = client.getSecretValue(r).secretString();
        return secret;
    }

    // //

    public static void main(String[] args) {
        AwsCredentialsProvider creds = StaticCredentialsProvider.create(
                AwsBasicCredentials.create("AKIA...", "key_name"));
        S3AsyncClient s3Client;
        try {
            s3Client = S3AsyncClient.builder()
                    .credentialsProvider(creds)
                    .region(Region.US_EAST_1)
                    .endpointOverride(URI.create("https://s3.amazonaws.com/"))
                    .build();
            CompletableFuture<GetObjectResponse> futureGet =
                    s3Client.getObject(
                            GetObjectRequest.builder()
                                    .bucket("bucket_name")
                                    .key("jdk-17_linux-x64_bin.tar.gz")
                                    .build(),
                            AsyncResponseTransformer.toFile(Paths.get("D:\\jdk17.gz")));
            futureGet.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

}
