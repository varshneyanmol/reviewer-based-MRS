package defaultPackage;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, IntWritable, Text>{
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String ratingRecord = value.toString();
		String[] ratingRecordSplit = ratingRecord.split(",");
		
		int movie_id = Integer.parseInt(ratingRecordSplit[1]);
		String reviewerRatingPair = ratingRecordSplit[0] + "," + ratingRecordSplit[2];
		
		context.write(new IntWritable(movie_id), new Text(reviewerRatingPair));
	}
}
