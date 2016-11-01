package bdp.stock;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


// Extend mapper class to use custom mapping function.
public class DailyMaxMapper extends Mapper<Object, DailyStock, Text, DailyStock> { 
	 
    // Map function for mapping key value pairs
    public void map(Object key, DailyStock value, Context context) throws IOException, InterruptedException {
        context.write(value.getCompany(), value);
    }
}
