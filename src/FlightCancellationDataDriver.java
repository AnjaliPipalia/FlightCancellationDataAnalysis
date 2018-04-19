import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FlightCancellationDataDriver {

	public static void main(String[] args) throws Exception {
		Job job3 = new Job(new Configuration());
		job3.setJarByClass(FlightCancellationDataDriver.class);
		job3.setNumReduceTasks(1);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(IntWritable.class);
		job3.setMapperClass(FlightCancellationDataMapper.class);
		job3.setReducerClass(FlightCancellationDataReducer.class);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job3, new Path(args[0]));
		FileOutputFormat.setOutputPath(job3, new Path(args[1]));
		job3.waitForCompletion(true);
	}
}
