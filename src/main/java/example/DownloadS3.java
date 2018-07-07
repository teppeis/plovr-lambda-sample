package example;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DownloadS3 implements RequestHandler<Integer, String> {
    @Override
    public String handleRequest(Integer input, Context context) {
        final LambdaLogger logger = context.getLogger();
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        final String bucketName = "com.teppeis.sample.hello-lambda";
        final String key = "closure.tgz";
        try {
            S3Object o = s3.getObject(bucketName, key);
            S3ObjectInputStream s3is = o.getObjectContent();
            final Path tempDirectory = Files.createTempDirectory(Paths.get("/tmp"), null);
            Files.copy(s3is, tempDirectory.resolve(key));
            s3is.close();
//            Files.list(tempDirectory).forEach(System.out::println);
            return "success";
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            throw new RuntimeException(e);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }
}