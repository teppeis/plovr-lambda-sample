package example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

public class HandleGateway implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {
    final ObjectMapper mapper = new ObjectMapper();

    @Override

    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent input, Context context) {
        final String method = input.getHttpMethod().toUpperCase();
        final APIGatewayProxyResponseEvent responseEvent = new APIGatewayProxyResponseEvent();
        try {
            switch (method) {
                case "POST":
                    Request request = mapper.readValue(input.getBody(), Request.class);
                    Response response = handlePost(request, context);
                    responseEvent.setStatusCode(200);
                    responseEvent.setBody(mapper.writeValueAsString(response));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported Method: " + method);
            }
            return responseEvent;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Response handlePost(Request request, Context context) {
        try {
            final Path targetDir = Files.createTempDirectory(Paths.get("/tmp"), null);
//            Files.list(Paths.get("/tmp")).forEach(System.out::println);
            final Response response = extractFromS3(targetDir, request.getKey(), context);
            this.cleanup(targetDir);
            return response;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Response extractFromS3(Path targetDir, final String key, Context context) throws IOException {
        final LambdaLogger logger = context.getLogger();
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        final String bucketName = System.getenv("BUCKET_NAME");
        logger.log("bucketName: " + bucketName);
        logger.log("key: " + key);
        try (S3Object s3o = s3.getObject(bucketName, key);
             InputStream s3i = s3o.getObjectContent();
             InputStream gzipi = new GzipCompressorInputStream(s3i);
             ArchiveInputStream i = new TarArchiveInputStream(gzipi);
        ) {

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
            return new Response("success");
        }
    }

    private void cleanup(Path targetDir) throws IOException {
        Files.walk(targetDir)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
    }
}