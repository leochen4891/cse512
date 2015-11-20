/**
 * CSE 512 Assignment 4
 * Lei Chen 1206139983
 */
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Equijoin {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] strs = line.split(",", 3);
			if (null == strs || strs.length < 2) {
				return;
			}
			String joining = strs[1].trim();
			word.set(joining);
			context.write(word, value);
			//System.out.println("---------------------------- mapping " + word.toString() + " to " + line);
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private Text empty = new Text();
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Map<String, List<String>> tables = new HashMap<String, List<String>>();
			
			/* NOTE duplication is possible, an input of 4 rows that have the same key will end up here
				R,1,a
				R,1,b
				S,1,c
				S,1,d
			produces output of 
				R,1,a, S,1,c
				R,1,a, S,1,d
				R,1,b, S,1,c
				R,1,b, S,1,d 
			*/
			for (Text t : values) {
				String line = t.toString();
				String[] strs = line.split(",", 3);
				String tableName = strs[0];
				List<String> table = tables.get(tableName);
				if (null == table) table = new LinkedList<String>();
				table.add(line);
				tables.put(tableName, table);
				//System.out.println("---------------------------- reduce " + "add " + line + " to " + tableName);
			}

			// only allow two tables
			if (tables.size() != 2) {
				return;
			}
			
			// Cartesian product of the two tables
			List<String> tb1 = null;
			List<String> tb2 = null;
			
			for (List<String> table : tables.values()) {
				if (tb1 == null) {
					tb1 = table;
				} 
				tb2 = table;
			}
			
			for (String l1 : tb1) {
				for (String l2 : tb2) {
					result.set(l1+ "," + l2);
					context.write(result, empty);
					//System.out.println("---------------------------- reduce " + "writting " + result.toString());
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Equijoin");
		job.setJarByClass(Equijoin.class);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}