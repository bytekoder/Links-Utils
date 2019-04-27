package cs.linksutil;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * @author bhavanishekhawat
 */
public class LinkReducer extends Reducer<Text, Text, Text, Text> {

    /**
     *
     * @param key the url coming from the mapper
     * @param value the tags coming from the mapper
     * @param context a context
     * @throws IOException
     * @throws InterruptedException
     */
    public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

        // Combining tags together
        StringBuilder sb = new StringBuilder();
        for (Text text1 : value) {
            sb.append(text1.toString() + " ");
        }

        // Removing duplicate tags
        String uniques = Arrays.stream(sb.toString().split(" ")).distinct().collect(Collectors.joining(" "));
        context.write(key, new Text(uniques));
    }
}

