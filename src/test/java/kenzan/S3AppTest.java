package kenzan;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import com.google.common.collect.Lists;


public class S3AppTest implements Serializable{

    private static final long serialVersionUID = -2812793962042547207L;

    @Test
    public void testApp() throws IOException {

        try(final InputStream inputStream = new FileInputStream("spark-s3.properties");
        final JavaSparkContext sc = JavaSparkContextLoader.loadFromInputStream(inputStream)){
            
            Configuration hadoopConf = sc.hadoopConfiguration();
            hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
            hadoopConf.set("fs.s3n.awsAccessKeyId","AKIAIFXFJG63KWYEK4BQ");
            hadoopConf.set("fs.s3n.awsSecretAccessKey","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");

            JavaRDD<String> javaRDD = sc.parallelize(Lists.newArrayList("Hi"));
            
            javaRDD.saveAsTextFile("s3n://kenzan.spark-fun/file.json");
            
        }        
        
    }
}
