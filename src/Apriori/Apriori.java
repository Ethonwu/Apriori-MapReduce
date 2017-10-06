package Apriori;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
	
 {
     private final static IntWritable one = new IntWritable(1);
     private Text word = new Text();
     private String t = new String();
     private String subsets = new String();
     
   /*  public void setup(Context context) throws IOException
     {
    	   //Add Feature for read file
         Path pt = new Path("hdfs:/ethonwu/Anotherfile.txt"); //Location on HDFS
         FileSystem fs = FileSystem.get(new Configuration());
         BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
         String line;
         line = br.readLine();
         while(line!=null)
         {
        	   
         }
         //end
     }*/
     
    
     public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
      {
    	 
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens())
          {
              t = itr.nextToken().toString();
              int list_l = t.length();
              StringTokenizer st = new StringTokenizer(t, ",");
              int[] list = new int[list_l];
              int k = 0;
              while (st.hasMoreTokens())
              {
	            //  word.set("Test:"+st.nextToken().toString());
                // context.write(word, one);
	            list[k] = Integer.valueOf(st.nextToken());
	            k++;
	          }
   
             int n = k;
             subsets = new String();
             char tmp = '\0';  
             for (int i = 0; i < (1<<n); i++)
              {       
	             subsets = new String();
	             subsets = "";
	             tmp = '\0';
                 for (int j = 0; j < n; j++)  
                  {        
    	      
    	                tmp = '\0';
                    if ((i & (1 << j)) > 0) 
                     {               
                    	   if(subsets == "")
        	   		        {
        	   			      subsets = subsets + Integer.toString(list[j]);
        	   		        }
                    	   else 
        	   		        {
        	   		          subsets = subsets +"," + Integer.toString(list[j]);
        	   		         }
                      }
                    }
               word.set(subsets);
               context.write(word, one);
                } 
    
   
             }
        /*
        //Add Feature for read file
        Path pt = new Path("hdfs:/ethonwu/Anotherfile.txt"); //Location on HDFS
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        line = br.readLine();
        while(line!=null)
        {
       	//   word.set(line);
       	 //  context.write(word,one);
       	   line = br.readLine();
        }
        //end
         * */
        }
      }
 
	

 public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> 
  {
	 private IntWritable result = new IntWritable();
	 private IntWritable test = new IntWritable(10);
     public void reduce(Text key, Iterable<IntWritable> values,Context context)throws IOException, InterruptedException
    
     {
    	   
    	    
     
       int sum = 0; 
       for (IntWritable val : values) 
       {
		 sum += val.get();
       }
       if (sum >= 3) 
       {
	      result.set(sum);   
	     context.write(key, result);
	   }
   
        
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

