//hadoop jar Covid19_1.jar /covid19_full_data.csv false /Covid19_1_false
//hadoop jar Covid19_1.jar /covid19_full_data.csv true /Covid19_1_true
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Covid19_1 {
	
	public class wordCountConstant{
		public static final int date = 0;
		public static final int country = 1;
		public static final int new_cases = 2;
		public static final int new_deaths = 3;
		
	}
	 static int flag;
	
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        Text add_country = new Text();
        IntWritable add_new_cases = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
        	
        	String[] line = value.toString().split(",");
        	
        	if(line[2].equals("new_cases")) {
        		return;
        	}
        	
        	if(!line[wordCountConstant.date].equals("2019-12-31"))
        	{
        		
        			add_country.set(line[wordCountConstant.country]);
            		add_new_cases.set(Integer.parseInt(line[wordCountConstant.new_cases]));
            		context.write(add_country, add_new_cases);
        		
        		
        	}
//            StringTokenizer itr = new StringTokenizer(value.toString());
//            while (itr.hasMoreTokens()) {
//                word.set(itr.nextToken());
//                context.write(word, one);
//            }
        	
        	
        }
        
    }

    public static class TokenMapper
    extends Mapper<Object, Text, Text, IntWritable>{

Text add_country = new Text();
IntWritable add_new_cases = new IntWritable();

public void map(Object key, Text value, Context context
) throws IOException, InterruptedException {
	
	String[] line = value.toString().split(",");
	
	if(line[2].equals("new_cases")) {
		return;
	}
	
	if(!line[wordCountConstant.date].equals("2019-12-31"))
	{
			if(!line[wordCountConstant.country].equals("World"))
			{
				add_country.set(line[wordCountConstant.country]);
        		add_new_cases.set(Integer.parseInt(line[wordCountConstant.new_cases]));
        		context.write(add_country, add_new_cases);
			}
		
	}
//    StringTokenizer itr = new StringTokenizer(value.toString());
//    while (itr.hasMoreTokens()) {
//        word.set(itr.nextToken());
//        context.write(word, one);
//    }
	
	
}

}
    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text country, Iterable<IntWritable> new_cases,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : new_cases) {
                sum += val.get();
            }
            result.set(sum);
            context.write(country, result);
        }
    }

    public static void main(String[] args) throws Exception {
    	if(args[1].equals("true"))
        {
        	flag=1;
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Covid19_1");
        job.setJarByClass(Covid19_1.class);
        if(args[1].equals("true"))
        {
        	job.setMapperClass(TokenizerMapper.class);
        }
        else if(args[1].equals("false"))
        {
        	job.setMapperClass(TokenMapper.class);
        }
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
