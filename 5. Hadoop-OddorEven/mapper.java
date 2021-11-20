package oddoreven;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> values, Reporter r) throws IOException {
        
		String data[] = value.toString().split(" "); 
        for (String num : data)  
        { 
            int number = Integer.parseInt(num); 
            if (number % 2 == 1)  
            { 
                values.collect(new Text("ODD"), new IntWritable(number)); 
            } 
            else 
            { 
                values.collect(new Text("EVEN"), new IntWritable(number)); 
            } 
		}
	}

}
