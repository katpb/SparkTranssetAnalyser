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

	public static void main(String[] args) {

		try (SparkSession sparkSession = SparkSession.builder().master("local").appName("TranssetAnalyser")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/txnDB.transactions2")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/txnDB.plus").getOrCreate();) {
			
			jsc = new JavaSparkContext(sparkSession.sparkContext());

			Dataset<Row> transactionDS = MongoSpark.load(jsc).toDF();
			transactionDS.printSchema();
			transactionDS.show();

			SparkTranssetAnalyser app = new SparkTranssetAnalyser();
			app.groupPLUByDate(sparkSession, transactionDS);
			app.groupNumberOfPLUSales(sparkSession, transactionDS);
			
			/*
			 * Add your query api's here.
			 */

		} finally {
			jsc.close();
			SparkSession.clearActiveSession();
		}

	}

	private void groupNumberOfPLUSales(SparkSession sparkSession, Dataset<Row> transactionDS) {

		Dataset<Row> trLines = transactionDS.select(col("trans.trHeader.trTickNum.trSeq").as("TxnNum"), col("trans.type").alias("tranType"),
				explode(col("trans.trLines.trLine")).as("trLines"));
		trLines.createOrReplaceTempView("itemlines");
		creatingNonRefundSales(sparkSession, transactionDS);
		deptSales(sparkSession);
		pluSales(sparkSession);
		
	}

	private void creatingNonRefundSales(SparkSession sparkSession, Dataset<Row> transactionDS) {
		Dataset<Row> nonRefundSales = sparkSession.sql("SELECT  * FROM itemlines WHERE tranType!=\"refund sale\" OR tranType!=\"refund network sale\"");	
		nonRefundSales.createOrReplaceTempView("nonRefundSales");
	}

	private void pluSales(SparkSession sparkSession) {
		Dataset<Row> trlUpcc = sparkSession.sql(
				"SELECT SUM(trLines.trlQty), trLines.trlUPC, trLines.trlDesc FROM itemLines WHERE trLines.trlUPC IS NOT NULL  GROUP BY trLines.trlUPC, trLines.trlDesc");
		trlUpcc.show();
	}

	private void deptSales(SparkSession sparkSession) {
		Dataset<Row> trlDept = sparkSession.sql(
				"SELECT COUNT(trLines.trlDept), trLines.trlDept.value, tranType FROM nonRefundSales WHERE tranType = \"sale\" OR tranType = \"network sale\" GROUP BY trLines.trlDept,tranType");
		trlDept.show();
		Dataset<Row> trlDept1 = sparkSession
				.sql("SELECT COUNT(trLines.trlDept), trLines.trlDept.value FROM nonRefundSales GROUP BY trLines.trlDept");
		trlDept1.show();
	}

	/**
	 * 
	 * Transforms the transset data from 'transactions' collections at txnDB and writes the 
	 * result data 
	 * 
	 * @param sparkSession
	 * @param transactionDS
	 */
	private void groupPLUByDate(SparkSession sparkSession, Dataset<Row> transactionDS) {

		Dataset<Row> trLines = transactionDS.select(substring(col("trans.trHeader.date"), 0, 10).as("trDate"),
				explode(col("trans.trLines.trLine")).as("trLine"));
		Dataset<Row> filteredPLUWithDate = trLines.filter(col("trLine.type").equalTo("plu"))
				.select(col("trDate"), col("trLine.trlDesc").as("trPluDesc")).sort(col("trDate").asc());
		Dataset<Row> groupByDateAndPlu = filteredPLUWithDate.groupBy("trPluDesc", "trDate").count();
		groupByDateAndPlu.show();
		
		writeToResultSetDB(groupByDateAndPlu, "groupPluByDate");
		
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
			writeConfigOverrides.put("database", "resultSetDB");
			writeConfigOverrides.put("writeConcern.w", "majority");
		}
		writeConfigOverrides.put("collection", collectionName);
	    WriteConfig writeConfig = WriteConfig.create(jsc).withOptions(writeConfigOverrides);
		MongoSpark.save(df, writeConfig);
	}
	


}
