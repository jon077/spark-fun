package kenzan;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;


public class JavaSparkContextLoader {

    
    public static JavaSparkContext loadFromInputStream(final InputStream inputStream) throws IOException {

        Properties properties = new Properties();
        properties.load(inputStream);
        final Set<String> propertyNames = properties.stringPropertyNames();
        
        final SparkConf sparkConf = new SparkConf();
        propertyNames.stream().forEach(name -> {
            if(name.startsWith("spark")){
                sparkConf.set(name, properties.getProperty(name));
            }
        });
        
        final JavaSparkContext context = new JavaSparkContext(sparkConf);
        properties.stringPropertyNames().stream().forEach(name -> {
            if(name.startsWith("fs.s3")){
                context.hadoopConfiguration().set(name, properties.getProperty(name));
            }
        });
        return context;
    }
}
