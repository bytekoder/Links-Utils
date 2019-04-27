package cs.linksutil;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * @author bhavanishekhawat
 */
public class LinkMapper extends Mapper<Object, Text, Text, Text> {

    private Text text = new Text();

    /**
     * Creates several maps with chunks of data based on common keys
     *
     * @param key     the offset in the file
     * @param value   the tags
     * @param context a context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        // Reading line by line
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        while (itr.hasMoreTokens()) {
            text.set(itr.nextToken());

            // Parse url and tags and set it in the attributes
            String urlKey = Link.parse(text.toString()).url();
            List<String> tags = Link.parse(text.toString()).tags();

            // Make sure url is not empty
            assert urlKey != null;

            // Write everything to context
            // Replacing trivial data with spaces
            context.write(new Text(urlKey),
                            new Text(tags.toString().replace(",", "").replace("[", "").replace("]", "").trim()));
        }
    }
}
