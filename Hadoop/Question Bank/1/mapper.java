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

public class mapper extends Mapper<Object, Text, Text, IntWritable>{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      
    //   StringTokenizer itr = new StringTokenizer(value.toString());
    //   while (itr.hasMoreTokens()) {
    //     word.set(itr.nextToken());
    //     context.write(word, one);
    //   }
    
        String s = value.toString();
        String s1 = s.substring(15,19);
        int temperature;
        if(s.charAt(87)=='+'){
            temperature = Integer.parseInt(s.substring(88, 92));
        }else{
            temperature = Integer.parseInt(s.substring(87, 92));
        }
        context.write(new Text(s1),new IntWritable(temperature));    
    }
  }