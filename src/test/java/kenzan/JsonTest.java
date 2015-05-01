package kenzan;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;


public class JsonTest implements Serializable{

    private static final long serialVersionUID = -3046654231380569159L;

    @Test
    public void testApp() {

        final String jsonFile = "src/test/resources/json";
        SparkConf conf = new SparkConf()
        .setMaster("local")
        .setAppName("Simple Application");
        
        final SQLContext sc = new SQLContext(new JavaSparkContext(conf));
        
        DataFrame data = sc.jsonFile(jsonFile);
        
        data.printSchema();
        
        // Register this SchemaRDD as a table.
        data.registerTempTable("result");
        
        DataFrame result = sc.sql(
            "SELECT status FROM result WHERE status = 'success'");
        
        System.out.println("count: " + result.count());

    }
}
