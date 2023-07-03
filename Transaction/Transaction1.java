package Transaction;

public class Transaction1 {
	
	public static class Map extends MapReduceBase implements
	Mapper < LongWritable , Text , Text , IntWritable > {
	private final static IntWritable one = new IntWritable (1);
	public void map ( LongWritable key , Text value ,
	OutputCollector < Text , IntWritable > output , Reporter reporter )
	throws IOException {
	String valueString = value . toString ();
	String [] data = valueString . split (" ," );
	// Column starts at 0 th index column 3 is username
	output . collect ( new Text ( data [2]) , one );
	}
	}
	
	public static class Reduce extends MapReduceBase
	implements Reducer < Text , IntWritable , Text , IntWritable > {
	public void reduce ( Text t_key , Iterator < IntWritable > values ,
	OutputCollector < Text , IntWritable > output , Reporter reporter )
	throws IOException {
	Text key = t_key ; int frequency = 0;
	while ( values . hasNext ()) {
	IntWritable value = ( IntWritable ) values . next ();
	frequency += value . get ();
	}
	output . collect ( key , new IntWritable ( frequency ));
	}
	}


	public static void main(String[] args) {
		// TODO Auto-generated method stub
		JobConf conf = new JobConf ( BankTransaction . class );
		conf . setJobName ( " BankTransaction " );
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
