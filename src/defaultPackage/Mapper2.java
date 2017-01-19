package defaultPackage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<Text, Text, Text, Text>{
	
	@Override
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		System.out.println("in mapper 2");
		String oneMovieAllRatingsCombined = value.toString();
		String[] reviewerRatingsPairs = oneMovieAllRatingsCombined.split(" ");
		
		String[] reviewerRatingPair;
		
		
		Map<String, String> reviewerRatingMap = new HashMap<String, String>();
		for (int i = 0; i < reviewerRatingsPairs.length; i++) {
			reviewerRatingPair = reviewerRatingsPairs[i].split(",");
			reviewerRatingMap.put(reviewerRatingPair[0], reviewerRatingPair[1]);
		}
		
		Set<String> reviewer_ids = reviewerRatingMap.keySet();
		Text reviewerPair = new Text();
		Text ratingPair = new Text();
		
		for (String reviewer_id1 : reviewer_ids) {
			for (String reviewer_id2 : reviewer_ids) {
		
				if (Integer.parseInt(reviewer_id1) < Integer.parseInt(reviewer_id2)) {
				
					reviewerPair.set(reviewer_id1 + "," + reviewer_id2);
					ratingPair.set(reviewerRatingMap.get(reviewer_id1) + "," + reviewerRatingMap.get(reviewer_id2));
					System.out.println(reviewerPair.toString() + "  " + ratingPair.toString());
					context.write(reviewerPair, ratingPair);
				}
			}
		}
		
	}
}
