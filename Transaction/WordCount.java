package Transaction;

public class WordCount {
	
	public static class Map extends MapReduceBase implements
	Mapper < LongWritable , Text , Text , IntWritable > {
	private final static IntWritable one = new IntWritable (1);
	private Text word = new Text ();
	public void map ( LongWritable key , Text value ,
	OutputCollector < Text , IntWritable > output , Reporter reporter )
	throws IOException {
	String line = value . toString ();
	StringTokenizer tokenizer = new StringTokenizer ( line );
	while ( tokenizer . hasMoreTokens ()) {
	word . set ( tokenizer . nextToken ());
	output . collect ( word , one );
	}
	}
	}
	
	public static class Reduce extends MapReduceBase
	implements Reducer < Text , IntWritable , Text , IntWritable > {
	public void reduce ( Text key , Iterator < IntWritable > values ,
	OutputCollector < Text , IntWritable > output , Reporter reporter )
	throws IOException {
	int sum = 0;
	while ( values . hasNext ()) {
	sum += values . next (). get ();
	}
	output . collect ( key , new IntWritable ( sum ));
	}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		JobConf conf = new JobConf ( WordCount . class );
		conf . setJobName ( " wordcount " );
		// Set the type of value we get at the output < Text , IntWritable >
		conf . setOutputKeyClass ( Text . class );
		conf . setOutputValueClass ( IntWritable . class );
		conf . setMapperClass ( Map . class ); // Set the Mapper class
		// Set the Reducer and combiner class
		conf . setCombinerClass ( Reduce . class );
		conf . setReducerClass ( Reduce . class );
		// Set the Input and Output Format class
		conf . setInputFormat ( TextInputFormat . class );
		conf . setOutputFormat ( TextOutputFormat . class );
		// Configure the input path and output path
		FileInputFormat . setInputPaths ( conf , new Path ( args [0]));
		FileOutputFormat . setOutputPath ( conf , new Path ( args [1]));
		JobClient . runJob ( conf ); // Run the JOB

	}

}
