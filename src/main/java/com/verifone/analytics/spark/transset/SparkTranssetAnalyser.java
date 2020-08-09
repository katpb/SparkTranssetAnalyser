package com.verifone.analytics.spark.transset;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.substring;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.to_utc_timestamp;

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
		Dataset<Row> trLines = transactionDS.select(to_date(to_utc_timestamp(col("trans.trHeader.date"),"YYYY-MM-DD")).as("trDate"),col("trans.type"), col("trans.trValue"),
				col("trans.trHeader.trTickNum.trSeq"),explode(col("trans.trLines.trLine")).as("trLines")).filter(col("trLines.type").equalTo("postFuel"));
		trLines.createOrReplaceTempView("itemlines");
		trLines.show();
		trLines.printSchema();
		Dataset<Row> salesByFuelProduct  = sparkSession
				.sql("SELECT trDate as Date,trLines.trlFuel.fuelProd.value as FuelProductDesc,sum(abs(trLines.trlLineTot)) as Amount,sum(abs(trLines.trlFuel.fuelVolume)) as Volume FROM itemlines  GROUP BY Date,FuelProductDesc ORDER BY Date desc").filter(col("Amount").gt(new Double("0.0")));
	   salesByFuelProduct.show();
	   writeToResultSetDB(salesByFuelProduct, "salesByFuelProduct");
		
	}

	private void customerWaitTimeByCashier(SparkSession sparkSession, Dataset<Row> transactionDS) {
		Dataset<Row> transactions = transactionDS.select(to_date(to_utc_timestamp(col("trans.trHeader.date"),"YYYY-MM-DD")).as("Date"),col("trans.trHeader.cashier.value").as("CashierName"),
				col("trans.trHeader.duration").as("WaitTime"));
		transactions.createOrReplaceTempView("transactionsByCashier");
		transactions.show();
		Dataset<Row> averageWaitTimeByCashier = sparkSession.sql("select Date,CashierName,avg(WaitTime) as AvgWaitTime from transactionsByCashier GROUP BY Date,CashierName Order By CashierName desc");
		averageWaitTimeByCashier.show();
		writeToResultSetDB(averageWaitTimeByCashier, "averageWaitTimeByCashier");
	}

	private void dailySalesCount(SparkSession sparkSession, Dataset<Row> transactionDS) {
		Dataset<Row> transactions = transactionDS.select(to_date(to_utc_timestamp(col("trans.trHeader.date"),"YYYY-MM-DD")).as("Date"),col("trans.type"));
		transactions.createOrReplaceTempView("transactionCounts");
		transactions.show();
		Dataset<Row> dailySalesCount  = sparkSession.sql("select Date,count(type) as TxnCount from transactionCounts GROUP BY Date order by Date desc");
		dailySalesCount .show();
		writeToResultSetDB(dailySalesCount ,"dailySalesCount");
		
	}

	private void averageTransactionAmount(SparkSession sparkSession, Dataset<Row> transactionDS) {
		Dataset<Row> transactions = transactionDS.select(to_date(to_utc_timestamp(col("trans.trHeader.date"),"YYYY-MM-DD")).as("Date"),col("trans.type"),
				col("trans.trValue.trTotWTax"));
		transactions.createOrReplaceTempView("transactionTotals");
		transactions.show();
		Dataset<Row> averageTransactionAmount = sparkSession.sql("select Date,avg(trTotWTax) as AvgAmount from transactionTotals GROUP BY Date order by Date desc");
		averageTransactionAmount.show();
		writeToResultSetDB(averageTransactionAmount,"averageTransactionAmount");
		
	}

	private void salesByMOP(SparkSession sparkSession, Dataset<Row> transactionDS) {
		Dataset<Row> trPayLines = transactionDS.select(to_date(to_utc_timestamp(col("trans.trHeader.date"),"YYYY-MM-DD")).as("trDate"),col("trans.type"),
				(explode(col("trans.trPaylines.trPayline")).as("trPaylines"))).filter(col("trPaylines.trpPayCode.value").notEqual("Change"));
		trPayLines.createOrReplaceTempView("paymentlines");
		trPayLines.show();
		trPayLines.printSchema();
		Dataset<Row> salesByMOP = sparkSession
				.sql("SELECT trDate as Date, trPayLines.trpPayCode.value as MopName, count(trPaylines) as Count FROM paymentlines Group By Date,MopName order by Date desc ");
		salesByMOP.show();
		writeToResultSetDB(salesByMOP, "salesByMOP");
		
	}

	private void salesByCardType(SparkSession sparkSession, Dataset<Row> transactionDS) {
		Dataset<Row> trPayLines = transactionDS.select(to_date(to_utc_timestamp(col("trans.trHeader.date"),"YYYY-MM-DD")).as("trDate"),col("trans.type"),
				(explode(col("trans.trPaylines.trPayline")).as("trPaylines"))).filter(col("trPaylines.trpPayCode.value").notEqual("Change"));
		trPayLines.createOrReplaceTempView("paymentlines");
		Dataset<Row> salesByCardType = sparkSession
				.sql("SELECT trDate as Date,trPayLines.trpCardInfo.trpcCCName.value as CardType,count(trPaylines) as Count FROM paymentlines GROUP By Date,CardType Order by Date desc");
		writeToResultSetDB(salesByCardType, "salesByCardType");
		
	}

	private void salesByEntryMethod(SparkSession sparkSession, Dataset<Row> transactionDS) {
		Dataset<Row> trPayLines = transactionDS.select(to_date(to_utc_timestamp(col("trans.trHeader.date"),"YYYY-MM-DD")).as("trDate"),col("trans.type"),
				(explode(col("trans.trPaylines.trPayline")).as("trPaylines"))).filter(col("trPaylines.trpPayCode.value").notEqual("Change"));
		trPayLines.createOrReplaceTempView("paymentlines");
		Dataset<Row> salesByEntryMethod = sparkSession
				.sql("SELECT trDate as Date,trPayLines.trpCardInfo.trpcEntryMeth as EntryMethod,count(trPaylines) as Count FROM paymentlines GROUP By Date,EntryMethod Order by Date desc");
		salesByEntryMethod.show();
		writeToResultSetDB(salesByEntryMethod, "salesByEntryMethod");
	}

	private void discountsByLoyaltyProgram(SparkSession sparkSession, Dataset<Row> transactionDS) {
		Dataset<Row> trLines = transactionDS.select(to_date(to_utc_timestamp(col("trans.trHeader.date"),"YYYY-MM-DD")).as("trDate"),col("trans.type"), col("trans.trValue"),
				explode(col("trans.trLines.trLine")).as("trLines"),col("trans.trHeader.trTickNum.trSeq"));
		trLines.createOrReplaceTempView("itemlines");
		trLines.show();
		trLines.printSchema();
		Dataset<Row> trLoyLines = trLines.select(col("trDate"),explode(col("trLines.trloPPGDisc")).as("trLoyLines"));
		trLoyLines.createOrReplaceTempView("loyaltyItemlines");
		trLoyLines.show();
		trLoyLines.printSchema();		
		Dataset<Row> discountsByLoyaltyProgram = sparkSession
				.sql("SELECT trDate as Date,trLoyLines.programID as LoyaltyProgram,sum(trLoyLines.PPGTotDisc) as DiscAmount FROM loyaltyItemlines Group By Date,LoyaltyProgram Order By Date desc");
		writeToResultSetDB(discountsByLoyaltyProgram, "discountsByLoyaltyProgram");
		
	}

	private void recurringCustomerCount(SparkSession sparkSession, Dataset<Row> transactionDS) {
		Dataset<Row> trPayLines = transactionDS.select(col("trans.type"),
				(explode(col("trans.trPaylines.trPayline")).as("trPaylines"))).filter(col("trPaylines.trpPayCode.value").notEqual("Change"));
		trPayLines.createOrReplaceTempView("paymentlines");
		Dataset<Row> recurringCustomerCount = (Dataset<Row>) sparkSession
				.sql("SELECT trPayLines.trpCardInfo.trpcAccount as CustCardNumber,count(trPaylines) as Count FROM paymentlines GROUP By trPayLines.trpCardInfo.trpcAccount  Order by Count desc").filter(col("CustCardNumber").isNotNull()).limit(10);
		writeToResultSetDB(recurringCustomerCount, "recurringCustomerCount");
	
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
