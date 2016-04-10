package main.java.nl.hu.hadoop.wordcount;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class WordCount {

	public static void main(String[] args) throws Exception {
		Job job = new Job();
		job.setJarByClass(WordCount.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
	}
}

class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable Key, Text value, Context context) throws IOException, InterruptedException {
		String[] words = value.toString().split(" ");
		for (String word : words){
			
			String wordResult = word.replaceAll("[-+.^:,()*&%!@#$]","");
			if (wordResult.length()>1){
				String reverseWord = new StringBuilder(wordResult.toLowerCase()).reverse().toString();
				char lastLetter = reverseWord.charAt(0);
				char firstLetter = wordResult.charAt(0);
				if (lastLetter == 'a' || lastLetter == 'e' || lastLetter == 'u' || lastLetter == 'i' || lastLetter == 'o'){
					if (firstLetter >= 'a' && firstLetter <= 'z'){
						context.write(new Text(wordResult), new IntWritable(1));
					}
				}
			}
		}
	}
}

class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int s = 0;
		for (IntWritable i : values) {
			s = i.get();
		}
		
		context.write(key, new IntWritable(s));
		
	}
}