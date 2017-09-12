package Apriori;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;
import org.apache.log4j.Category;
import org.apache.log4j.Logger;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class Apriori {
	public static class TokenizerMapper 
	
    extends Mapper<Object, Text, Text, IntWritable>{
 private final static IntWritable one = new IntWritable(1);
 private Text word = new Text();
 private String l = new String();
 //private StringBuffer t = new StringBuffer();
 private String t = new String();
 private String subsets = new String();
// private String subs =new String();
 private static int flag = 0;
 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
   StringTokenizer itr = new StringTokenizer(value.toString());
   while (itr.hasMoreTokens())
   {
  // l = itr.nextToken().toString();
   //t.append(l.split(" "));
   t = itr.nextToken().toString();
   int n = t.length();
   subsets = new String();
   String[] subs = null;
   char tmp = '\0';  
   StringTokenizer st = null;
   for (int i = 0; i < (1<<n); i++)
   {       
	   subsets = new String();
	   subsets = "[";
	   // subsets += ",";
  	 	tmp = '\0';
       for (int j = 0; j < n; j++)  
       {        
    	      
    	       tmp = '\0';
           if ((i & (1 << j)) > 0) 
           {               
          	 	 tmp = t.charAt(j);
       		     subsets = subsets + Character.toString(tmp) + " ";
           }
          
       }
       subsets += "]";
       word.set(subsets);
       context.write(word, one);
       
  
   } 
    
   // subs = subsets.split(",");
   //   st = new StringTokenizer(subsets, ",");
    //for (String d : subs) 
    //{
   // 	 flag++;
      //word.set(d);
   //while (st.hasMoreTokens()){
	 //  word.set(st.nextToken());
	   //context.write(word,one);
	//}
    	 // word.set(subsets+" "+Integer.toString(flag));
     //context.write(word,one);
    //}
   
 }
 }
 }
 
	

 public static class IntSumReducer 
    extends Reducer<Text,IntWritable,Text,IntWritable> {
 private IntWritable result = new IntWritable();

 public void reduce(Text key, Iterable<IntWritable> values, 
                    Context context
                    ) throws IOException, InterruptedException {
   int sum = 0;
   for (IntWritable val : values) {
     sum += val.get();
   }
   if (sum >= 3) {
	   result.set(sum);   
	   context.write(key, result);
   }
   //result.set(sum);  
   
 }
}
 
public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    Path outputPath = new Path(args[1]);
    outputPath.getFileSystem(conf).delete(outputPath, true);
    job.setJarByClass(Apriori.class);
    job.setMapperClass(TokenizerMapper.class);
   // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    
}
  }

