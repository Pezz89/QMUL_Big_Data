package bdp.stock;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;


public class CompanyMinMaxReducer extends Reducer<Text, DailyStock, Text, Text> {

    private Text result = new Text();

    public void reduce(Text key, Iterable<DailyStock> values, Context context)

              throws IOException, InterruptedException {

        double maxVal = Double.NEGATIVE_INFINITY;
        double minVal = Double.POSITIVE_INFINITY;

        for (DailyStock value : values) {
            if (value.getHigh().get() > maxVal)
                maxVal = value.getHigh().get();
            if (value.getLow().get() < minVal)
                minVal = value.getLow().get();
        }

        String resultString = "MIN: " + minVal + "  MAX: " + maxVal;

        result.set(resultString);

        context.write(key, result);
    }
}
