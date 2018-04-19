import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightCancellationDataMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] lineSplit = line.split(",");
		if (lineSplit[22] != null && !lineSplit[21].equals("NA")
				&& !lineSplit[22].equals("NA") && !lineSplit[21].equals("Cancelled")
				&& !lineSplit[22].equals("CancellationCode")) {
			context.write(new Text(lineSplit[22]), new IntWritable(Integer.parseInt(lineSplit[21])));
		}
	}
}
