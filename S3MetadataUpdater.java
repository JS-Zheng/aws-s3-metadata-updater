import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.util.List;

/**
 * Created by zhengzhongsheng on 2017/5/8.
 */
public class S3MetadataUpdater {

    public static final int FORMAT_UNSUPPORTED = 0;
    public static final int FORMAT_JPEG = 1;
    public static final int FORMAT_PNG = 2;

    public static final String bucketName = "your-bucket-name";

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


            ObjectListing ol = s3.listObjects(bucketName);
            List<S3ObjectSummary> objects = ol.getObjectSummaries();

            for (S3ObjectSummary os : objects) {
                updateContentType(s3, os);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    static void updateContentType(AmazonS3 s3, S3ObjectSummary os) {
        String fileKey = os.getKey();
        System.out.println("* " + os.getKey());

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

            CopyObjectRequest request = new CopyObjectRequest(bucketName, fileKey, bucketName, fileKey)
                    .withNewObjectMetadata(newMeta);

            s3.copyObject(request);
        }
    }

    static String getContentTypeValue(int format) {
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

