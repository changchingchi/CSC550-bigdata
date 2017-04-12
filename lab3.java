package edu.itu.csc550;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Lab3 extends Configured implements Tool {
	static Configuration conf = new Configuration();
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      conf.set("textinputformat.record.delimiter","\r");
      int res = ToolRunner.run(conf, new Lab3(), args);
      
      System.exit(res);
   }

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = Job.getInstance(conf,"Lab3");
      job.setJarByClass(Lab3.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);

      MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class,Map.class);
      MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class,Map2.class);

      FileOutputFormat.setOutputPath(job, new Path(args[2]));

      job.waitForCompletion(true);
      
      return 0;
   }
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	      
	      @Override
	      public void map(LongWritable key, Text value, Context context)
	              throws IOException, InterruptedException {
	    	  //This is for users table
	    	  String[] tokenArray = value.toString().trim().split("\t");
	    	  if(tokenArray.length==5){
	    	  	  context.write(new Text(tokenArray[0]), new Text(
	    	  			
	    	  			  "\t"+tokenArray[1]+
		    			  "\t"+tokenArray[2]+
		    			  "\t"+tokenArray[3]+
		    			  "\t"+tokenArray[4]));
	    	  }
	      }
	   }
   public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
	      
	      @Override
	      public void map(LongWritable key, Text value, Context context)
	              throws IOException, InterruptedException {
	    	  //This is for posts table
	    	  String[] tokenArray = value.toString().split("\t");
	    	  if(tokenArray.length==19)
	    	  {
	    	  context.write(new Text(tokenArray[3]),new Text(
	    			 
	    			  	  "\t"+tokenArray[0]+
		    			  "\t"+tokenArray[1]+
		    			  "\t"+tokenArray[2]+
		    			  "\t"+tokenArray[5]+
		    			  "\t"+tokenArray[6]+
		    			  "\t"+tokenArray[7]+
		    			  "\t"+tokenArray[8]+
		    			  "\t"+tokenArray[9]
		    			  ));
	    	  }
	      }
	   }

	   public static class Reduce extends Reducer<Text, Text, Text, Text> {
		   
	      @Override
	      public void reduce(Text key, Iterable<Text> values, Context context)
	              throws IOException, InterruptedException {
	         StringBuffer sb = new StringBuffer();
	         String[] user_array=null;
	         String[] post_array=null;
	         Set<String> data = new HashSet<String>();
	         
	         for (Text val : values) {
	        	 data.add(val.toString().trim());
//	        	if(val.toString().trim().split("\t").length==4){
//	        		user_array = val.toString().trim().split("\t");
//	        	}else if(val.toString().trim().split("\t").length==8){
//	        		post_array =  val.toString().trim().split("\t");
//	        	}
	         }
	         for(String val:data){
	        	 String[] TokenArray = val.toString().trim().split("\t");
	        	 if(TokenArray.length==4){
	        	   user_array = TokenArray;
	        	 }
	         }
	         for(String val:data){
	        	 String[] TokenArray = val.toString().trim().split("\t");
	        	 if(TokenArray.length==8){
	    	         context.write(key, new Text(val+"\t"+Arrays.toString(user_array)));

	        	 }
	         }
	         
//	         	sb = sb.insert(0, "\t\n");
//	        	sb = sb.append("\n");
	         for (Text val : values) {
	        	 context.write(key, new Text(Arrays.toString(post_array)+Arrays.toString(user_array)));
	         }
//	         context.write(key, new Text(sb.toString()));
	         
	      }
	   }
}
