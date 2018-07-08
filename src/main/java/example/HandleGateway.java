package example;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.google.gson.Gson;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class HandleGateway implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    final Gson gson = new Gson();

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent input, Context context) {
        final String method = input.getHttpMethod().toUpperCase();
        Response response;
        switch (method) {
            case "POST":
                final Request request = gson.fromJson(input.getBody(), Request.class);
                response = handlePost(request, context);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported Method: " + method);
        }
        final APIGatewayProxyResponseEvent responseEvent = new APIGatewayProxyResponseEvent();
        responseEvent.setStatusCode(200);
        responseEvent.setBody(gson.toJson(response));
        return responseEvent;
    }

    private Response handlePost(Request input, Context context) {
        final LambdaLogger logger = context.getLogger();
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        final String bucketName = System.getenv("BUCKET_NAME");
        logger.log("bucketName: " + bucketName);
        final String key = input.getKey();
        logger.log("key: " + key);
        try (S3Object s3o = s3.getObject(bucketName, key);
             InputStream s3i = s3o.getObjectContent();
             InputStream gzipi = new GzipCompressorInputStream(s3i);
             ArchiveInputStream i = new TarArchiveInputStream(gzipi);
        ) {
            final Path targetDir = Files.createTempDirectory(Paths.get("/tmp"), null);

            logger.log("start extracting");
            ArchiveEntry entry = null;
            while ((entry = i.getNextEntry()) != null) {
                if (!i.canReadEntryData(entry)) {
                    logger.log("cannot read entry data: " + entry);
                    continue;
                }
                final Path target = targetDir.resolve(entry.getName());
                if (entry.isDirectory()) {
                    if (!Files.isDirectory(target) && !target.toFile().mkdirs()) {
                        throw new IOException("failed to create directory " + target);
                    }
                } else {
                    final Path parent = target.getParent();
                    if (!Files.isDirectory(parent) && !parent.toFile().mkdirs()) {
                        throw new IOException("failed to create directory " + parent);
                    }
                    Files.copy(i, target);
                }
            }

            logger.log("extracted");
            Files.list(Paths.get("/tmp")).forEach(System.out::println);
            return new Response("success");
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            throw new RuntimeException(e);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }
}