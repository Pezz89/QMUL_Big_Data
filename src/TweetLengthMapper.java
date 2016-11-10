import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class TweetLengthMapper extends Mapper<Object, Text, IntWritable, IntWritable> { 

	private IntWritable value = new IntWritable(1);
    private IntWritable feature = new IntWritable();

    public void map(Object key, Text input_val, Context context) throws IOException, InterruptedException {
   
        // For each input tweet...
        // Seperate raw line into an array of it's elements (seperated in the
        // file by ';')
    	String[] line = input_val.toString().split(";");

        if(line.length == 4) {
            // Calculate the length of the tweet
            feature.set(line[2].length());

            // Output the length of the tweet as the key and 1 as the value
            context.write(feature,value);
        }
    }
}
