package kenzan;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;


public class S3WriteApp implements Serializable{

    private static final long serialVersionUID = -4393882020055445682L;

    @Test
    public void testApp() throws IOException {

        try(final InputStream inputStream = new FileInputStream("spark-s3.properties");
             final JavaSparkContext sc = JavaSparkContextLoader.loadFromInputStream(inputStream)){
            
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
