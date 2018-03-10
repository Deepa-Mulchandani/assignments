import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FilterInvalid 
{
public static class Ymap extends Mapper <LongWritable, Text, LongWritable, Text>
{
	
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
	
	String line = value.toString();
	String [] list = line.split("|");

	if(!(list[0].equals("NA") || list[1].equals("NA")))
	{
		context.write(key, value);
	}
	}
}
	

public static void main(String[] args) throws Exception
{
	Configuration conf = new Configuration();

    Job job = new Job(conf, "FilterInvalid");

    job.setJarByClass(FilterInvalid.class);

    job.setMapperClass(Ymap.class);

    job.setOutputKeyClass(LongWritable.class);

    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));

    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}

