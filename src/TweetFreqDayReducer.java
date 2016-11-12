import java.io.IOException;

import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TweetFreqDayReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    private LongWritable result = new LongWritable();

    public void reduce(Text key, Iterable<LongWritable> values, Context context)

              throws IOException, InterruptedException {

        long sum = 0;

        for (LongWritable value : values) {
            sum+=value.get();
            if(sum < 0) {
                throw new RuntimeException("Overflow error in sum");
            }
        }

        result.set(sum);

        context.write(key,result);
    }
}
