package example;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ExtractTgz implements RequestHandler<Integer, String> {
    @Override
    public String handleRequest(Integer input, Context context) {
        final LambdaLogger logger = context.getLogger();
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        final String bucketName = "com.teppeis.sample.hello-lambda";
        final String key = "closure.tgz";
        try (S3Object s3o = s3.getObject(bucketName, key);
             InputStream s3i = s3o.getObjectContent();
             InputStream gzipi = new GzipCompressorInputStream(s3i);
             ArchiveInputStream i = new TarArchiveInputStream(gzipi);
        ) {
            final Path targetDir = Files.createTempDirectory(Paths.get("/tmp"), null);

            ArchiveEntry entry = null;
            logger.log("start extracting");
            while ((entry = i.getNextEntry()) != null) {
                if (!i.canReadEntryData(entry)) {
                    // log something?
                    logger.log("cannot read entry data: " + entry);
                    continue;
                }
                final Path target = targetDir.resolve(entry.getName());
//                logger.log("target: " + target);
                if (entry.isDirectory()) {
//                    logger.log("isDirectory");
                    if (!Files.isDirectory(target) && !target.toFile().mkdirs()) {
                        throw new IOException("failed to create directory " + target);
                    }
                } else {
//                    logger.log("isFile");
                    final Path parent = target.getParent();
                    if (!Files.isDirectory(parent) && !parent.toFile().mkdirs()) {
                        throw new IOException("failed to create directory " + parent);
                    }
//                    logger.log("copying");
                    Files.copy(i, target);
//                    logger.log("copied");
                }
            }

            logger.log("extracted");
            Files.list(targetDir).forEach(System.out::println);
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