import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightCancellationDataReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
	LongWritable result = new LongWritable();
	long max = 0L;
	Text maxCancellationCode = new Text();

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		long sum = 0;

		for (IntWritable val : values) {
			sum += val.get();
		}
		if (sum >= max) {
			maxCancellationCode.set(key);
			max = sum;
		}
		result.set(sum);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		context.write(new Text("The most common reason for flight cancellations : (A = carrier, B = weather, C = NAS, D = security)"), null);
		context.write(maxCancellationCode, new LongWritable(max));
	}
}
