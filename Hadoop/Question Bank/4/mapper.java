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

public class mapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // StringTokenizer itr = new StringTokenizer(value.toString());
        // while (itr.hasMoreTokens()) {
        // word.set(itr.nextToken());
        // context.write(word, one);
        // }

        Configuration conf = context.getConfiguration();
        int m = Integer.parseInt(conf.get("m"));
        int p = Integer.parseInt(conf.get("p"));
        String line = value.toString();
        String[] indicesAndValue = line.split(",");
        String outputKey;
        String outputValue;
        if (indicesAndValue[0].equals("A")) {
            for (int i = 0; i < p; i++) {
                outputKey = indicesAndValue[1] + "," + i;
                outputValue = "A," + indicesAndValue[2] + "," + indicesAndValue[3];
                context.write(new Text(outputKey),new Text(outputValue));
            }
        } else {
            for (int i = 0; i < m; i++) {
                outputKey = i + "," + indicesAndValue[2];
                outputValue = "B," + indicesAndValue[1] + "," + indicesAndValue[3];
                context.write(new Text(outputKey),new Text(outputValue));
            }
        }

    }
}
