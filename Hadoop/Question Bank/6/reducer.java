
package code;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        // int sum = 0;
        // for (IntWritable val : values) {
        //     sum += val.get();
        // }
        // result.set(sum);
        // context.write(key, result);


        // double s=Double.MIN_VALUE;
        // for(DoubleWritable val:values){
        //     s = Math.max(s, val.get()) ;
        // }
        // context.write(key,new DoubleWritable(s));

        int count=0;
        double s=0;
        for(DoubleWritable val:values){
            count+=1;
            s += val.get();
        }
        double n = s/count;
        context.write(key,new DoubleWritable(n));
    }
}
