package kenzan;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;


public class AppTest implements Serializable{

    private static final long serialVersionUID = -1402074515821224734L;

    @Test
    public void testApp() throws IOException {

        String logFile = "/Applications/spark-1.3.0/README.md"; // Should be some file on your system
        try(final InputStream inputStream = ClassLoader.getSystemResourceAsStream("spark-local.properties");
            final JavaSparkContext sc = JavaSparkContextLoader.loadFromInputStream(inputStream)){
        
            JavaRDD<String> logData = sc.textFile(logFile).cache();
    
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
