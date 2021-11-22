package insurance;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class reducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key, Iterator<IntWritable> value1, OutputCollector<Text, IntWritable> values, Reporter r) throws IOException {
            int frequencyForCountry = 0;
            while (value1.hasNext()) {
                frequencyForCountry += value1.next().get();
            }
            values.collect(key, new IntWritable(frequencyForCountry));
	}
}
