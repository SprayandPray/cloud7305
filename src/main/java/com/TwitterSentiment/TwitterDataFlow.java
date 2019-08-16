package com.TwitterSentiment;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import static com.TwitterSentiment.StanfordSentiment.*;


public class TwitterDataFlow {

	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("Incorrect number of arguments! \n");
			System.out.println(
					"Usage: Input Output \n " + "Input: Location where the partitioned data needs to be read from"
							+ "Output: Where the final result needs to be stored");
			System.exit(1);
		}
		
		SparkSession spark = SparkSession.builder().appName("Sentiment Analyzer").getOrCreate();
		spark.sparkContext().hadoopConfiguration().set("mapreduce.input.fileinputformat.input.dir.recursive", "true");

		String inputPath = args[0];
		Dataset<Row> data = spark.read().json(inputPath);

		spark.sqlContext().udf().register("Sentiment", (String s) -> GetSentiment(s), DataTypes.DoubleType);

		// We only analyze Tweets about Microsoft since it takes much time for analyzing even one company in the list
		List<String> list = Arrays.asList("microsoft");
		// List<String> list = Arrays.asList("apple", "microsoft", "google", "facebook");

		String query;
		String outPath;
		String outPathIntermediate;
		
		Dataset<Row> result;
		
		for (String company : list) {
		        outPath = args[1] + "/" + company;
		        outPathIntermediate = args[1] + "/intermediate/" + company;
		
		        // tmp1 extracts TimeStamp, partitionBy (Date), tweet text, tweet text in lower
		        // case and followers_count of the user tweeting
		        data.createOrReplaceTempView("complete");
		        Dataset<Row> tmp1 = spark.sql(
					                // "Jul 09 2019 13:21:00" timestamp example
					                // "Jul 09" partitionBy example
					                "select concat(substr(created_at,5,6), substr(created_at,26,5),' ',substr(created_at,12,6),'00') as timestamp,"
					                + "substr(created_at,5,6) as partitionBy,text,lower(text) as main_text,user.followers_count as followers from complete");
		
		        // Filtering tweets having certain company names in it
			tmp1.persist(StorageLevel.MEMORY_ONLY_SER_2());
			
			tmp1.createOrReplaceTempView("tmp");
		        Dataset<Row> tmp2 = spark.sql("select * from tmp where main_text regexp '(" + company + ")'");
		

			tmp2.persist(StorageLevel.MEMORY_ONLY_SER_2());
			// Creates a view named twitter
		        tmp2.createOrReplaceTempView("twitter");
		
		        // tmp3 contains the entire selected data along with the Sentiment value of the
		        // tweets
		        Dataset<Row> tmp3 = spark.sql("select  *, Sentiment(text) as seVal from twitter");
		
		//	tmp3.cache();
			tmp3.write().partitionBy("partitionBy").mode("append").json(outPathIntermediate);
		
		        tmp3.persist(StorageLevel.MEMORY_ONLY_SER_2());
			// Creating another view
		        tmp3.createOrReplaceTempView("dataSe");
		
		        // Get NetSentiment using the number of followers as the weight
		        Dataset<Row> net = spark.sql("select  *,followers*seVal as NetSentiment from dataSe");
		
		        
			
		//	net.cache();
			net.persist(StorageLevel.MEMORY_ONLY_SER_2());
			// Creating a final view to save the data
		        net.createOrReplaceTempView("final");
		
		        // Averaging the Sentiment Values per minute by grouping the data onto it
		        query = "select timestamp,partitionBy,AVG(NetSentiment) from final group by timestamp,partitionBy";
		
		        // Saving the result in result dataset
		        result = spark.sql(query);
		
		        // Writing the result onto the disk
		        result.write().partitionBy("partitionBy").mode("append").json(outPath);
		}
}
}

