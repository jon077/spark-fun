package kenzan;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;


public class S3WriteApp implements Serializable{

    @Test
    public void testApp() {

        SparkConf conf = new SparkConf()
        .setMaster("local")
        .setAppName("Simple Application");
        try(JavaSparkContext sc = new JavaSparkContext(conf)){
            
            Configuration hadoopConf = sc.hadoopConfiguration();
            hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem");
            hadoopConf.set("fs.s3n.awsAccessKeyId","YYY");
            hadoopConf.set("fs.s3n.awsSecretAccessKey","XXXX");
        
            
            JavaRDD<String> logData = sc.textFile("s3n://kenzan.spark-fun/README.md").cache();

            long numAs = logData.filter(new Function<String, Boolean>() {
                private static final long serialVersionUID = -4598771113192827740L;

            public Boolean call(String s) { return s.contains("a"); }
            }).count();

            long numBs = logData.filter(new Function<String, Boolean>() {
                private static final long serialVersionUID = -7990468526595507141L;

            public Boolean call(String s) { return s.contains("b"); }
            }).count();

            System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
            
        }        
        
    }
}
