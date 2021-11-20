package oddoreven;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class reducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key, Iterator<IntWritable> value1, OutputCollector<Text, IntWritable> values, Reporter r) throws IOException {
            int sum = 0, count = 0; 
            if (key.equals("ODD"))  
            { 
                while (value1.hasNext()) 
                { 
                    sum += value1.next().get(); 
                    count++; 
                } 
            } 
            else 
            { 
                while (value1.hasNext())  
                { 
                    sum += value1.next().get(); 
                    count++; 
                } 
            } 
            values.collect(key, new IntWritable(sum)); 
            values.collect(key, new IntWritable(count)); 
	}
}
