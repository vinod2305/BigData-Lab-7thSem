package code;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        if (key.charAt(0) == '1') {
            int cnt = 0;
            for (IntWritable val : values) {
                cnt += 1;
            }
            String k = "Frequency "+key;
            context.write(new Text(k), new IntWritable(cnt));
        } else {
            int cnt = 0;
            for (IntWritable val : values) {
                cnt += 1;
            }
            String k = "country "+key;
            context.write(new Text(k), new IntWritable(cnt));
        }

    }
}
