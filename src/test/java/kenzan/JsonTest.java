package kenzan;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.junit.Test;

public class JsonTest implements Serializable {

    private static final long serialVersionUID = -3046654231380569159L;

    @Test
    public void testApp() throws IOException {

        final String jsonFile = "src/test/resources/json";
        try (final InputStream inputStream = ClassLoader.getSystemResourceAsStream("spark-local.properties");
            final JavaSparkContext sc = JavaSparkContextLoader.loadFromInputStream(inputStream)) {

            final SQLContext sql = new SQLContext(sc);
            DataFrame data = sql.jsonFile(jsonFile);

            data.printSchema();

            // Register this SchemaRDD as a table.
            data.registerTempTable("result");

            DataFrame result = sql.sql("SELECT status FROM result WHERE status = 'success'");

            System.out.println("count: " + result.count());
        }

    }
}
