package insurance;
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> values, Reporter r) throws IOException {
        String valueString = value.toString();
		String[] SingleCountryData = valueString.split(",");
		values.collect(new Text(SingleCountryData[16]), new IntWritable(1));
	}

}
