import java.io.IOException;
        import java.net.URI;
        import java.io.BufferedReader;
        import java.io.FileReader;
        import java.util.HashMap;
        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.Path;
        import org.apache.hadoop.io.DoubleWritable;
        import org.apache.hadoop.io.IntWritable;
        import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Job;
        import org.apache.hadoop.mapreduce.Mapper;
        import org.apache.hadoop.mapreduce.Reducer;
        import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Covid19_3{

    public class Index{
        public static final int date = 0;
        public static final int country = 1;
        public static final int new_cases = 2;
        public static final int new_deaths = 3;

    }
    private static HashMap<String, Long> PopulationMap = new HashMap<String, Long>();
    public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable>{


        Text add_country = new Text();
        DoubleWritable add_new_cases = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().split(",");

            if(line[2].equals("new_cases")) {
                return;
            }

            if(!line[Index.date].equals("2019-12-31"))
            {

                add_country.set(line[Index.country]);
                add_new_cases.set(Double.parseDouble(line[Index.new_cases]));

                context.write(add_country, add_new_cases);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        private BufferedReader brReader;
        @Override
        public void setup(Context context) throws IOException{

            URI[] files = context.getCacheFiles();
            String strLineRead = "";
            for (URI file : files){

                Path f = new Path(file);
                brReader = new BufferedReader(new FileReader(f.getName()));

                // Read each line, split and load to HashMap
                while ((strLineRead = brReader.readLine()) != null) {
                    String[] popFieldArray = strLineRead.split(",");
                    if(popFieldArray.length==5)
                    {

                        if(popFieldArray[4].equals("population"))
                        {
                            continue;
                        }
                        PopulationMap.put(popFieldArray[1],Long.parseLong(popFieldArray[4]));
                    }
                }
                System.out.println(PopulationMap);
            }

        }

        public void reduce(Text country, Iterable<DoubleWritable> new_cases,Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : new_cases) {
                sum += val.get();

            }

            if(PopulationMap.containsKey(country.toString()) && !country.toString().equals("World") && !country.toString().equals("International"))
            {
                double ans=0.0;


                ans = (sum/(double)(PopulationMap.get(country.toString())))*1000000;
                
                result.set(ans);
                context.write(country,result);

            }
            
        }
    }

    public static void main(String[] args) throws Exception {
    	long starttime=System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Covid19_3");
        job.addCacheFile(new Path(args[1]).toUri());
        job.setJarByClass(Covid19_3.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        long endtime = System.currentTimeMillis();
        System.out.println("Time :");
        System.out.println(endtime-starttime);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
        
    }
}