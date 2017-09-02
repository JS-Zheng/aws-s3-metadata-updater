import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.Date;
import java.util.List;

/**
 * Created by zhengzhongsheng on 2017/5/8.
 * https://blog.jason.party
 * <br>
 * Please import AWS Java SDK first!
 * http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-install.html
 */
public class S3MetadataUpdater {

    private static final int FORMAT_UNSUPPORTED = 0;
    private static final int FORMAT_JPEG = 1;
    private static final int FORMAT_PNG = 2;

    // Please update these fields
    private static final String YOUR_BUCKET_NAME = "your-bucket-name";
    private static final String YOUR_BUCKET_PATH = "wp-content/uploads/";
    private static final long MAX_AGE = 31536000L;

    public static void main(String args[]) {


        try {
            /*
             * The ProfileCredentialsProvider will return your [default]
             * credential profile by reading from the credentials file located at
             * (~/.aws/credentials).
             */
            AmazonS3 s3 = AmazonS3ClientBuilder.standard()
                    .withRegion(Regions.AP_NORTHEAST_1)
                    .build();


            System.out.println("===========================================");
            System.out.println("Getting Started with Amazon S3");
            System.out.println("===========================================\n");

            ObjectListing ol = s3.listObjects(YOUR_BUCKET_NAME, YOUR_BUCKET_PATH);
            List<S3ObjectSummary> objects = getObjectsExclude1000Limits(s3, ol);


            for (S3ObjectSummary os : objects) {
                /*
                 * do something.
                 */
                updateContentType(s3, os);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    // Prevent S3 1000 limits.
    static List<S3ObjectSummary> getObjectsExclude1000Limits(AmazonS3 s3, ObjectListing ol) {
        List<S3ObjectSummary> result = ol.getObjectSummaries();
        while (ol.isTruncated()) {
            ol = s3.listNextBatchOfObjects(ol);
            result.addAll(ol.getObjectSummaries());
        }
        return result;
    }


    // It's just example. Please use your custom rules.
    public static void updateContentType(AmazonS3 s3, S3ObjectSummary os) {
        String fileKey = os.getKey();
        System.out.println("* " + fileKey);

        int format = FORMAT_UNSUPPORTED;

        if (fileKey.contains(".jpg")) {
            format = FORMAT_JPEG;
        } else if (fileKey.contains(".png")) {
            format = FORMAT_PNG;
        }

        if (format != FORMAT_UNSUPPORTED) {

            String ContentTypeValue = getContentTypeValue(format);

            ObjectMetadata newMeta = new ObjectMetadata();
            newMeta.setContentType(ContentTypeValue);
            newMeta.setCacheControl("max-age=" + String.valueOf(MAX_AGE));
            newMeta.setHttpExpiresDate(new Date(getExpirationTimeByMaxAge(MAX_AGE)));

            CopyObjectRequest request = new CopyObjectRequest(YOUR_BUCKET_NAME, fileKey, YOUR_BUCKET_NAME, fileKey)
                    .withNewObjectMetadata(newMeta);

            s3.copyObject(request);
        }
    }

    private static long getExpirationTimeByMaxAge(long age) {
        Date now = new Date();
        return now.getTime() + age * 1000;
    }

    private static String getContentTypeValue(int format) {
        if (!isSupported(format))
            throw new IllegalArgumentException("unsupported");

        String result;
        if (format == FORMAT_PNG) {
            result = "image/png";
        } else {
            result = "image/jpeg";
        }

        return result;
    }


    private static boolean isSupported(int format) {
        return format == FORMAT_JPEG || format == FORMAT_PNG;
    }

}

