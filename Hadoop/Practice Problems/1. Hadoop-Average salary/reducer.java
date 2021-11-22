package average;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class reducer extends MapReduceBase implements Reducer<Text,FloatWritable,Text,Text>{
		public void reduce(Text key, Iterator<FloatWritable> value1, OutputCollector<Text, Text> values, Reporter r) throws IOException {
			
            float total = (float) 0;
            int count = 0;
            while(value1.hasNext()){
                total += value1.next().get();
                count++;
			}
            float avg = (float) total / count;
            String out = "Total: " + total + " :: " + "Average: " + avg;
			values.collect(key, new Text(out));
	}
}
