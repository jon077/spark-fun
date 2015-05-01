package kenzan;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;

import com.google.common.collect.Lists;


public class S3AppTest implements Serializable{

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

            JavaRDD<String> javaRDD = sc.parallelize(Lists.newArrayList("Hi"));
            
            javaRDD.saveAsTextFile("s3n://kenzan.spark-fun/file.json");
            
        }        
        
    }
}
