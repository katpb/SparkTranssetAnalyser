package com.verifone.analytics.spark.transset;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.substring;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;

/**
 * This spark application is  
 *
 */
public class SparkTranssetAnalyser {
	
	private static JavaSparkContext jsc = null;
	private static boolean initializeWriteConfig = false;
	private static Map<String, String> writeConfigOverrides = null; 
	private static String siteId = "txndb";

	public static void main(String[] args) {
		// Get site Id from the arug
		siteId = args[0];
		
		try (SparkSession sparkSession = SparkSession.builder().master("local").appName("TranssetAnalyser")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/txnDB."+siteId) // e.g., txnDB.site1
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/txnDB.plus").getOrCreate();) {
			
			jsc = new JavaSparkContext(sparkSession.sparkContext());

			Dataset<Row> transactionDS = MongoSpark.load(jsc).toDF();
			transactionDS.printSchema();
			transactionDS.show();

			SparkTranssetAnalyser app = new SparkTranssetAnalyser();
			
			app.salesByUPCTop(sparkSession, transactionDS);
			app.salesByUPCBottom(sparkSession, transactionDS);
			app.salesByCategoryTop(sparkSession, transactionDS);
			app.salesByCategoryBottom(sparkSession, transactionDS);
			app.salesByDepartmentTop(sparkSession, transactionDS);
			app.salesByDepartmentBottom(sparkSession, transactionDS);
			app.salesByFuelProduct(sparkSession, transactionDS);
			app.customerWaitTimeByCashier(sparkSession, transactionDS);
			app.dailySalesCount(sparkSession, transactionDS);
			app.averageTransactionAmount(sparkSession, transactionDS);
			app.salesByMOP(sparkSession, transactionDS);
			app.salesByCardType(sparkSession, transactionDS);
			app.salesByEntryMethod(sparkSession, transactionDS);
			app.discountsByLoyaltyProgram(sparkSession, transactionDS);
			app.recurringCustomerCount(sparkSession, transactionDS);

		} finally {
			jsc.close();
			SparkSession.clearActiveSession();
		}

	}
	
	/**
	 * 
	 * Transforms the transset data from 'transactions' collections at txnDB and writes the 
	 * result data 
	 * 
	 * @param sparkSession
	 * @param transactionDS
	 */
	private void salesByUPCTop(SparkSession sparkSession, Dataset<Row> transactionDS) {

		Dataset<Row> trLines = transactionDS.select(substring(col("trans.trHeader.date"), 0, 10).as("trDate"),
				explode(col("trans.trLines.trLine")).as("trLine"));
		Dataset<Row> filteredPLUWithDate = trLines.filter(col("trLine.type").equalTo("plu"))
				.select(col("trDate"), col("trLine.trlDesc").as("trPluDesc")).sort(col("trDate").asc());
		Dataset<Row> salesByUpcDF = filteredPLUWithDate.groupBy("trPluDesc", "trDate").count();
		salesByUpcDF.show();
		
		writeToResultSetDB(salesByUpcDF, "salesByUPC");
		
	}

	private void salesByUPCBottom(SparkSession sparkSession, Dataset<Row> transactionDS) {
		// TODO Auto-generated method stub
		
	}

	private void salesByCategoryTop(SparkSession sparkSession, Dataset<Row> transactionDS) {
		// TODO Auto-generated method stub
		
	}

	private void salesByCategoryBottom(SparkSession sparkSession, Dataset<Row> transactionDS) {
		// TODO Auto-generated method stub
		
	}

	private void salesByDepartmentTop(SparkSession sparkSession, Dataset<Row> transactionDS) {
		// TODO Auto-generated method stub
		
	}

	private void salesByDepartmentBottom(SparkSession sparkSession, Dataset<Row> transactionDS) {
	// TODO Auto-generated method stub
	
	}

	private void salesByFuelProduct(SparkSession sparkSession, Dataset<Row> transactionDS) {
		// TODO Auto-generated method stub
		
	}

	private void customerWaitTimeByCashier(SparkSession sparkSession, Dataset<Row> transactionDS) {
		// TODO Auto-generated method stub
		
	}

	private void dailySalesCount(SparkSession sparkSession, Dataset<Row> transactionDS) {
		// TODO Auto-generated method stub
		
	}

	private void averageTransactionAmount(SparkSession sparkSession, Dataset<Row> transactionDS) {
		// TODO Auto-generated method stub
		
	}

	private void salesByMOP(SparkSession sparkSession, Dataset<Row> transactionDS) {
		// TODO Auto-generated method stub
		
	}

	private void salesByCardType(SparkSession sparkSession, Dataset<Row> transactionDS) {
		// TODO Auto-generated method stub
		
	}

	private void salesByEntryMethod(SparkSession sparkSession, Dataset<Row> transactionDS) {
		// TODO Auto-generated method stub
		
	}

	private void discountsByLoyaltyProgram(SparkSession sparkSession, Dataset<Row> transactionDS) {
		// TODO Auto-generated method stub
		
	}

	private void recurringCustomerCount(SparkSession sparkSession, Dataset<Row> transactionDS) {
	// TODO Auto-generated method stub
	
	}

	/**
	 * Common method to write to resultSetDB.
	 * 
	 * @param df - Data frame object containing the filtered data.
	 * @param collectionName - name of a new collection that will be created at the DB.
	 */
	private static void writeToResultSetDB(Dataset<Row> df, String collectionName) {
		if(!initializeWriteConfig) {
			writeConfigOverrides = new HashMap<String, String>();
			writeConfigOverrides.put("database", siteId+"ResultsDB"); // site1ResultsDB
			writeConfigOverrides.put("writeConcern.w", "majority");
		}
		writeConfigOverrides.put("collection", collectionName);
	    WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeConfigOverrides);
		MongoSpark.save(df, writeConfig);
	}
	


}
