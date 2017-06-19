/*20135179 김다은*/


package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


public class freqOfWord extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new freqOfWord(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        
        Job job = Job.getInstance(getConf());
        job.setJarByClass(freqOfWord.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        job.setMapperClass(MapFindWord.class);
        job.setReducerClass(ReducecntWord.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setNumReduceTasks(10);
        job.waitForCompletion(true);
        return 0;
    }
    
    public static class MapFindWord extends Mapper<LongWritable, Text, Text,IntWritable> {
        private Text word = new Text();
        
        private final static IntWritable one = new IntWritable(1);
        String [] saveWord = {"text", "refers", "edition", "unavailable", "print", "one", "business", "book", "read", "series"};
        @Override
        public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        	try {
        		JSONObject jsonObject = new JSONObject(value.toString());
        		ArrayList<String> compareWord= new ArrayList<String>();
        		
        		// 주어진 word를 담은 배열 . 
                String[] findWord = jsonObject.getString("description").split("\\s+");
                
                //array_list에 넣기. 
                for(int i = 0;i<findWord.length;i++){
                	compareWord.add(findWord[i]); 
                }
                
                //해당 문장과 비교해서 출력.  
                
                for(int i = 0;i<saveWord.length;i++){
                	if(compareWord.contains(saveWord[i])==true)
                		context.write(new Text(saveWord[i]+", "+findWord[i]), new IntWritable(1));
                	
                }
            } 
        	catch (JSONException j) {
                j.printStackTrace();
            }
        }
    }
    
    public static class ReducecntWord extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
            int cnt = 0;
            for (IntWritable val : values) {
               cnt++;
            }
            context.write(key, new IntWritable(cnt));
            
        }
    }
}
