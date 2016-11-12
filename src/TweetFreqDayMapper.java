import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Date;
import java.util.Arrays;
import java.lang.NumberFormatException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class TweetFreqDayMapper extends Mapper<Object, Text, Text, LongWritable> { 

    // Long used for convenience and to avoid casting during the reducer
    // summation. This helps avoid potential integer overflows with very large
    // datasets.
	private LongWritable value = new LongWritable(1);
    private Text feature = new Text();
    // Define start and end dates of Olympics
    private static Date eventStart = new Date(116, 7, 5);
    private static Date eventEnd = new Date(116, 7, 22, 23, 59);

    public static String strJoin(String[] aArr, String sSep) {
        // Method taken from: http://stackoverflow.com/questions/1978933/a-quick-and-easy-way-to-join-array-elements-with-a-separator-the-opposite-of-sp
        StringBuilder sbStr = new StringBuilder();
        for (int i = 0, il = aArr.length; i < il; i++) {
            if (i > 0)
                sbStr.append(sSep);
            sbStr.append(aArr[i]);
        }
        return sbStr.toString();
    }

    public void map(Object key, Text input_val, Context context) throws IOException, InterruptedException {
   
        // For each input tweet...
        // Seperate raw line into an array of it's elements (seperated in the
        // file by ';')
    	String[] line = input_val.toString().split(";");

        // Filter out invalid entries that aren't complete
        if(line.length == 4) {
            // Get a date object for the tweet date
            try {
                // Maintained to raise error if data is malformed
                Long.parseLong(line[0]);

                // Code adapted from: http://stackoverflow.com/questions/10432543/extract-hash-tag-from-string
                Pattern hashtag_pattern = Pattern.compile("#(\\w+)");
                Matcher mat = hashtag_pattern.matcher(line[2]);
                while (mat.find()) {
                    feature.set(mat.group(1));
                    context.write(feature,value);
                }
            }
            // If data is not formed correctly then skip it
            catch (NumberFormatException e) {
                return;
            }
        }
    }
}
