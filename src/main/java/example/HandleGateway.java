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
import org.apache.http.HttpStatus;
import org.plovr.cli.Command;

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
        try {
            switch (method) {
                case "POST":
                    Request request = mapper.readValue(input.getBody(), Request.class);
                    return handlePost(request, context);
                default:
                    final APIGatewayProxyResponseEvent responseEvent = new APIGatewayProxyResponseEvent();
                    responseEvent.setStatusCode(HttpStatus.SC_METHOD_NOT_ALLOWED);
                    return responseEvent;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private APIGatewayProxyResponseEvent handlePost(Request request, Context context) {
        final APIGatewayProxyResponseEvent responseEvent = new APIGatewayProxyResponseEvent();
        Path targetDir = null;
        try {
            targetDir = Files.createTempDirectory(Paths.get("/tmp"), null);
//            Files.list(Paths.get("/tmp")).forEach(System.out::println);
            extractFromS3(targetDir, request.getKey(), context);
            final int status = compilePlovr(targetDir.resolve(request.getTargetConfigPath()));
            final Response response;
            if (status == 0) {
                responseEvent.setStatusCode(HttpStatus.SC_OK);
                response = new Response("success");
            } else {
                responseEvent.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                response = new Response("error");
            }
            responseEvent.setBody(mapper.writeValueAsString(response));
            return  responseEvent;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (targetDir != null) {
                this.cleanup(targetDir);
            }
        }
    }

    private int compilePlovr(Path targetConfigPath) {
        final Command command = Command.BUILD;
        final String[] args = {targetConfigPath.toString()};
        try {
            return command.execute(args);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void extractFromS3(Path targetDir, final String key, Context context) throws IOException {
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
        }
    }

    private void cleanup(Path targetDir) {
        try {
            Files.walk(targetDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static class Request {
        private String key;
        private String targetConfigPath;

        public Request() {
        }

        public Request(String key, String targetConfigPath) {
            this.key = key;
            this.targetConfigPath = targetConfigPath;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public String getTargetConfigPath() {
            return targetConfigPath;
        }

        public void setTargetConfigPath(String targetConfigPath) {
            this.targetConfigPath = targetConfigPath;
        }
    }
}