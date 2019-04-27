package cs.linksutil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author bhavanishekhawat
 */
public class LinkMapReduceDriver {

    public static void main(String[] args) throws Exception {

        // Configuration
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "Link Collector");
        job.setJarByClass(LinkMapReduceDriver.class);

        // Setting appropriate classes
        job.setMapperClass(LinkMapper.class);
        job.setCombinerClass(LinkReducer.class);
        job.setReducerClass(LinkReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Driver args
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
