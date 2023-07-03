package Transaction;

import Transaction.Transaction1.Map;
import Transaction.Transaction1.Reduce;

public class BankTransaction {
	
	public static class Map extends MapReduceBase implements
	Mapper < LongWritable , Text , Text , IntWritable > {
	private final static IntWritable one = new IntWritable (1);
	public void map ( LongWritable key , Text value ,
	OutputCollector < Text , IntWritable > output , Reporter reporter )
	throws IOException {
	// Convert the line to String
	String valueString = value . toString ();
	// Split the String with the delimiter " ,"
	String [] data = valueString . split (" ," );
	// Collect the output as < Username ( Col -3) , Amount ( Col -4) >
	output . collect ( new Text ( data [2]) ,
	new IntWritable ( Integer . parseInt ( data [3])));
	}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer < Tepublic void reduce ( Text t_key , Iterator < IntWritable > values ,
			OutputCollector < Text , IntWritable > output , Reporter reporter )
			throws IOException {
			// Copy the Key
			Text key = t_key ;
			// Initialize the amount = 0
			int tcount = 0;
			// Iterate through the values < Iterator > and convert the int
			// values to IntWritable
			while ( values . hasNext ()) {
			IntWritable value = ( IntWritable ) values . next ();
			tcount += value . get (); // Sum
			}
			// Collect the output as < Username , AmountSum >
			output . collect ( key , new IntWritable ( tcount ));
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
