package com.verifone.analytics.spark.transset;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.substring;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.to_utc_timestamp;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
//import com.verifone.analytics.LinearRegression;
//import com.verifone.analytics.LinearRegressionModel;
//import com.verifone.analytics.StringIndexer;
//import com.verifone.analytics.VectorAssembler;

/**
 * This spark application is  
 *
 */
public class SparkTranssetAnalyser {
	
	private static final int N = 10;
	private static final String COUNT_DEPT = "count(Dept)";
	private static final String SUM_QTY = "sum(Qty)";
	private static final String COUNT = "Count";
	
	private static final String GROUP_BY_DATE_CAT_VALUE_CAT_NUMBER = "\" GROUP BY Date, Cat.value, Cat.number, Qty";
	private static final String SELECT_DATE_CAT_VALUE_CAT_NUMBER_COUNT_FROM_NON_REFUND_SALES_WHERE_CAT_IS_NOT_NULL_AND_DATE =
			"SELECT Date, Cat.number As CategoryNum, Cat.value As CategoryDesc, SUM(Qty) FROM nonRefundSales WHERE Cat IS NOT NULL AND Date=\"";
	private static final String GROUP_BY_DATE_DEPT_NUMBER_DEPT_VALUE = "\" GROUP BY Date, Dept.number, Dept.value";
	private static final String SELECT_DATE_DEPT_NUMBER_DEPT_VALUE_COUNT_DEPT_FROM_NON_REFUND_SALES_WHERE_DATE = 
			"SELECT Date, Dept.number As DeptNum, Dept.value As DeptDesc, COUNT(Dept) FROM nonRefundSales WHERE Date=\"";

	private static final String SELECT_DATE_UPC_NUM_UPC_DESC_SUM_QTY_FROM_NON_REFUND_SALES_WHERE_DATE =
			"SELECT Date, UPCNum, UPCDesc, SUM(Qty) FROM nonRefundSales WHERE Date=\""; 
	private static final String AND_UPC_NUM_IS_NOT_NULL_GROUP_BY_DATE_UPC_NUM_UPC_DESC_QTY = 
			"\" AND UPCNum IS NOT NULL GROUP BY Date, UPCNum, UPCDesc, Qty";

	private static JavaSparkContext jsc = null;
	private static boolean initializeWriteConfig = false;
	private static Map<String, String> writeConfigOverrides = null; 
	private static String siteId = "txndb";

	public static void main(String[] args) {
		// Get site Id from the arug
		Logger.getLogger("org").setLevel(Level.ERROR); // this will prevent INFO logs, on console
		siteId = args[0];
		
		try(SparkSession sparkSession = SparkSession.builder().master("local").appName("TranssetAnalyser")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/txnDB."+siteId) // e.g., txnDB.site1
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/txnDB.plus").getOrCreate();) {

			jsc = new JavaSparkContext(sparkSession.sparkContext());

			Dataset<Row> transactionDS = MongoSpark.load(jsc).toDF();
			transactionDS.printSchema();
			transactionDS.show();

			SparkTranssetAnalyser app = new SparkTranssetAnalyser();
			
			String trDate = "Date";
			Dataset<Row> trLines = renamingColumsCreatingItemLinesView(transactionDS, trDate);
			app.creatingNonRefundSales(sparkSession, transactionDS);
			Dataset<Row> distinctDatesDataSet = trLines.select(col(trDate)).sort(col(trDate)).distinct();
			app.salesByUPCTop(sparkSession, N, distinctDatesDataSet);
			app.salesByUPCBottom(sparkSession, N, distinctDatesDataSet);
			app.salesByCategoryTop(sparkSession, N, distinctDatesDataSet);
			app.salesByCategoryBottom(sparkSession, N, distinctDatesDataSet);
			app.salesByDepartmentTop(sparkSession, N, distinctDatesDataSet);
			app.salesByDepartmentBottom(sparkSession, N, distinctDatesDataSet);
			
			app.salesByFuelProduct(sparkSession, transactionDS);
			app.customerWaitTimeByCashier(sparkSession, transactionDS);
			app.dailySalesCount(sparkSession, transactionDS);
			app.averageTransactionAmount(sparkSession, transactionDS);
			app.salesByMOP(sparkSession, transactionDS);
			app.salesByCardType(sparkSession, transactionDS);
			app.salesByEntryMethod(sparkSession, transactionDS);
			app.discountsByLoyaltyProgram(sparkSession, transactionDS);
			app.recurringCustomerCount(sparkSession, transactionDS);
			app.linearRegression(sparkSession);
		} catch (Exception e) {
		} finally {
		}
		jsc.close();
		SparkSession.clearActiveSession();
	}

	private void creatingNonRefundSales(SparkSession sparkSession, Dataset<Row> transactionDS) {
		Dataset<Row> nonRefundSales = sparkSession.sql(
				"SELECT Date, TxnNum, UPCNum, UPCDesc, Dept, Cat, Qty, tranType trLines FROM itemlines WHERE tranType!=\"refund sale\" OR tranType!=\"refund network sale\" OR tranType!=\"void\"");
		nonRefundSales.createOrReplaceTempView("nonRefundSales");
	}

	private static Dataset<Row> renamingColumsCreatingItemLinesView(Dataset<Row> transactionDS, String trDate) {
		Dataset<Row> trLines = transactionDS.select(substring(col("trans.trHeader.date"), 0, 10).as(trDate),
				col("trans.trHeader.trTickNum.trSeq").as("TxnNum"),
				col("trLines.trlUPC").as("UPCNum"),
				col("trLines.trlDesc").alias("UPCDesc"),
				col("trLines.trlDept").as("Dept"),
				col("trLines.trlCat").as("Cat"),
				col("trLines.trlQty").as("Qty"), col("trans.type").alias("tranType"),
				explode(col("trans.trLines.trLine")).as("trLines"));
		trLines.createOrReplaceTempView("itemlines");
		return trLines;
	}	

	private void linearRegression(SparkSession sparkSession) {
		Dataset<Row> csvData = sparkSession.read().option("header", "true").option("inferSchema", "true").format("csv")
				.load("Book1.csv");
		Dataset<Row> formattedDf = new StringIndexer().setInputCol("Day").setOutputCol("DayVal").fit(csvData)
				.transform(csvData);
		Dataset<Row> finalDf = new VectorAssembler()
				.setInputCols(new String[] { "Discount", "TotCustCount", "DayVal", "IndoorCustCount" })
				.setOutputCol("features").transform(formattedDf).withColumnRenamed("QtySold", "label")
				.select("label", "features");
		Dataset<Row>[] splits = finalDf.randomSplit(new double[] { 0.9, 0.1 });
		Dataset<Row> trainingData = splits[0];
		Dataset<Row> holdOutData = splits[1];

		LinearRegressionModel lrModel = new LinearRegression().fit(trainingData);

		Dataset<Row> predictions = lrModel.transform(holdOutData);

		predictions.select("label", "prediction", "features").show(false);
		
	}


	/**
	 * 
	 * Transforms the transset data from 'transactions' collections at txnDB and writes the 
	 * result data 
	 * 
	 * @param sparkSession
	 * @param n 
	 * @param transactionDS
	 */
	private void salesByUPCTop(SparkSession sparkSession, int n, Dataset<Row> distinctDatesDataSet) {
		System.out.println("+++++++++++++Date-wise Top Plu Sales+++++++++++++++++++++++++++");
		List<Row> distinctDatesList = distinctDatesDataSet.collectAsList();
		Iterator<Row> it = distinctDatesList.iterator();
		String firstDate = it.next().mkString();
		System.out.println(SELECT_DATE_UPC_NUM_UPC_DESC_SUM_QTY_FROM_NON_REFUND_SALES_WHERE_DATE + firstDate
				+ AND_UPC_NUM_IS_NOT_NULL_GROUP_BY_DATE_UPC_NUM_UPC_DESC_QTY);
		Dataset<Row> intermediateResult1 = sparkSession
				.sql(SELECT_DATE_UPC_NUM_UPC_DESC_SUM_QTY_FROM_NON_REFUND_SALES_WHERE_DATE + firstDate
						+ AND_UPC_NUM_IS_NOT_NULL_GROUP_BY_DATE_UPC_NUM_UPC_DESC_QTY)
				.sort(col(SUM_QTY).desc()).limit(n).withColumnRenamed(SUM_QTY, COUNT);
		Dataset<Row> salesByUpcTop = intermediateResult1;
		while (it.hasNext()) {
			String date = it.next().mkString();
			Dataset<Row> intermediateResult2 = sparkSession
					.sql(SELECT_DATE_UPC_NUM_UPC_DESC_SUM_QTY_FROM_NON_REFUND_SALES_WHERE_DATE + date
							+ AND_UPC_NUM_IS_NOT_NULL_GROUP_BY_DATE_UPC_NUM_UPC_DESC_QTY)
					.sort(col(SUM_QTY).desc()).limit(n).withColumnRenamed(SUM_QTY, COUNT);
			salesByUpcTop = salesByUpcTop.union(intermediateResult2);
		}
		salesByUpcTop.show();
		writeToResultSetDB(salesByUpcTop, "salesByUpcTop");
		
	}

	private void salesByUPCBottom(SparkSession sparkSession, int n, Dataset<Row> distinctDatesDataSet) {
		System.out.println("+++++++++++++Date-wise bottom plu Sales+++++++++++++++++++++++++++");
		List<Row> distinctDatesList = distinctDatesDataSet.collectAsList();
		Iterator<Row> it = distinctDatesList.iterator();
		String firstDate = it.next().mkString();
		Dataset<Row> intermediateResult1 = sparkSession
				.sql(SELECT_DATE_UPC_NUM_UPC_DESC_SUM_QTY_FROM_NON_REFUND_SALES_WHERE_DATE + firstDate
						+ AND_UPC_NUM_IS_NOT_NULL_GROUP_BY_DATE_UPC_NUM_UPC_DESC_QTY)
				.sort(col(SUM_QTY).asc()).limit(n).withColumnRenamed(SUM_QTY, COUNT);
		Dataset<Row> salesByUpcBottom = intermediateResult1;
		while (it.hasNext()) {
			String date = it.next().mkString();
			Dataset<Row> intermediateResult2 = sparkSession
					.sql(SELECT_DATE_UPC_NUM_UPC_DESC_SUM_QTY_FROM_NON_REFUND_SALES_WHERE_DATE + date
							+ AND_UPC_NUM_IS_NOT_NULL_GROUP_BY_DATE_UPC_NUM_UPC_DESC_QTY)
					.sort(col(SUM_QTY).asc()).limit(n).withColumnRenamed(SUM_QTY, COUNT);
			salesByUpcBottom = salesByUpcBottom.union(intermediateResult2);
		}
		salesByUpcBottom.show();
		writeToResultSetDB(salesByUpcBottom, "salesByUpcBottom");
		
	}

	private void salesByCategoryTop(SparkSession sparkSession, int n, Dataset<Row> distinctDatesDataSet) {
		System.out.println("+++++++++++++Date-wise top cat Sales+++++++++++++++++++++++++++");
		List<Row> distinctDatesList = distinctDatesDataSet.collectAsList();
		Iterator<Row> it = distinctDatesList.iterator();
		String date = it.next().mkString();
		Dataset<Row> intermediateResult1 = sparkSession
				.sql(SELECT_DATE_CAT_VALUE_CAT_NUMBER_COUNT_FROM_NON_REFUND_SALES_WHERE_CAT_IS_NOT_NULL_AND_DATE + date
						+ GROUP_BY_DATE_CAT_VALUE_CAT_NUMBER)
				.sort(col(SUM_QTY).desc()).limit(n).withColumnRenamed(SUM_QTY, COUNT);
		Dataset<Row> salesByCategoryTop = intermediateResult1;
		while (it.hasNext()) {
			String datee = it.next().mkString();
			Dataset<Row> intermediateResult2 = sparkSession
					.sql(SELECT_DATE_CAT_VALUE_CAT_NUMBER_COUNT_FROM_NON_REFUND_SALES_WHERE_CAT_IS_NOT_NULL_AND_DATE
							+ datee + GROUP_BY_DATE_CAT_VALUE_CAT_NUMBER)
					.sort(col(SUM_QTY).desc()).limit(n).withColumnRenamed(SUM_QTY, COUNT);
			salesByCategoryTop = salesByCategoryTop.union(intermediateResult2);
		}
		salesByCategoryTop.show();
		writeToResultSetDB(salesByCategoryTop, "salesByCategoryTop");
		System.out.println("---------------------------------------------------------------");
		
	}

	private void salesByCategoryBottom(SparkSession sparkSession, int n, Dataset<Row> distinctDatesDataSet) {
		System.out.println("+++++++++++++Date-wise bottom cat Sales+++++++++++++++++++++++++++");
        List<Row> distinctDatesList = distinctDatesDataSet.collectAsList();
        Iterator<Row> it = distinctDatesList.iterator();
        String date = it.next().mkString();
        Dataset<Row> intermediateResult1 = sparkSession
                     .sql(SELECT_DATE_CAT_VALUE_CAT_NUMBER_COUNT_FROM_NON_REFUND_SALES_WHERE_CAT_IS_NOT_NULL_AND_DATE + date
                                  + GROUP_BY_DATE_CAT_VALUE_CAT_NUMBER)
                      .sort(col(SUM_QTY).asc()).limit(n).withColumnRenamed(SUM_QTY, COUNT);
        Dataset<Row> salesByCategoryBottom = intermediateResult1;
        while (it.hasNext()) {
               String datee = it.next().mkString();
               Dataset<Row> intermediateResult2 = sparkSession
                            .sql(SELECT_DATE_CAT_VALUE_CAT_NUMBER_COUNT_FROM_NON_REFUND_SALES_WHERE_CAT_IS_NOT_NULL_AND_DATE + datee
                                         + GROUP_BY_DATE_CAT_VALUE_CAT_NUMBER)
                            .sort(col(SUM_QTY).asc()).limit(n).withColumnRenamed(SUM_QTY, COUNT);
               salesByCategoryBottom = salesByCategoryBottom.union(intermediateResult2);
        }
        salesByCategoryBottom.show();
    	writeToResultSetDB(salesByCategoryBottom, "salesByCategoryBottom");
        System.out.println("---------------------------------------------------------------");
	}

	private void salesByDepartmentTop(SparkSession sparkSession, int n, Dataset<Row> distinctDatesDataSet) {
		System.out.println("+++++++++++++Date-wise Top dept Sales+++++++++++++++++++++++++++");
		List<Row> distinctDatesList = distinctDatesDataSet.collectAsList();
		Iterator<Row> it = distinctDatesList.iterator();
		String date = it.next().mkString();
		Dataset<Row> intermediateResult1 = sparkSession
				.sql(SELECT_DATE_DEPT_NUMBER_DEPT_VALUE_COUNT_DEPT_FROM_NON_REFUND_SALES_WHERE_DATE + date
						+ GROUP_BY_DATE_DEPT_NUMBER_DEPT_VALUE).sort(col(COUNT_DEPT).desc()).limit(n).withColumnRenamed(COUNT_DEPT, COUNT);
		Dataset<Row> salesByDepartmentTop = intermediateResult1;
		while (it.hasNext()) {
		date = it.next().mkString();
		Dataset<Row> topnDeptperDay = sparkSession
				.sql(SELECT_DATE_DEPT_NUMBER_DEPT_VALUE_COUNT_DEPT_FROM_NON_REFUND_SALES_WHERE_DATE + date
						+ GROUP_BY_DATE_DEPT_NUMBER_DEPT_VALUE).sort(col(COUNT_DEPT).desc()).limit(n).withColumnRenamed(COUNT_DEPT, COUNT);;
		salesByDepartmentTop = salesByDepartmentTop.union(topnDeptperDay);
		}
		salesByDepartmentTop.show();
		writeToResultSetDB(salesByDepartmentTop, "salesByDepartmentTop");
	}

	private void salesByDepartmentBottom(SparkSession sparkSession, int n, Dataset<Row> distinctDatesDataSet) {
		System.out.println("+++++++++++++Date-wise bottom dept Sales+++++++++++++++++++++++++++");
		List<Row> distinctDatesList = distinctDatesDataSet.collectAsList();
		Iterator<Row> it = distinctDatesList.iterator();
		String date = it.next().mkString();
		Dataset<Row> intermediateResult1 = sparkSession
				.sql(SELECT_DATE_DEPT_NUMBER_DEPT_VALUE_COUNT_DEPT_FROM_NON_REFUND_SALES_WHERE_DATE + date
						+ GROUP_BY_DATE_DEPT_NUMBER_DEPT_VALUE).sort(col(COUNT_DEPT).asc()).limit(n).withColumnRenamed(COUNT_DEPT, COUNT);
		Dataset<Row> salesByDepartmentBottom = intermediateResult1;
		while (it.hasNext()) {
		date = it.next().mkString();
		Dataset<Row> topnDeptperDay = sparkSession
				.sql(SELECT_DATE_DEPT_NUMBER_DEPT_VALUE_COUNT_DEPT_FROM_NON_REFUND_SALES_WHERE_DATE + date
						+ GROUP_BY_DATE_DEPT_NUMBER_DEPT_VALUE).sort(col(COUNT_DEPT).asc()).limit(n).withColumnRenamed(COUNT_DEPT, COUNT);
		salesByDepartmentBottom = salesByDepartmentBottom.union(topnDeptperDay);
		}
		salesByDepartmentBottom.show();
		writeToResultSetDB(salesByDepartmentBottom, "salesByDepartmentBottom");
	
	}

	private void salesByFuelProduct(SparkSession sparkSession, Dataset<Row> transactionDS) {
		Dataset<Row> trLines = transactionDS.select(substring(col("trans.trHeader.date"), 0, 10).as("trDate"),col("trans.type"), col("trans.trValue"),
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
		Dataset<Row> transactions = transactionDS.select(substring(col("trans.trHeader.date"), 0, 10).as("Date"),col("trans.trHeader.cashier.value").as("CashierName"),
				col("trans.trHeader.duration").as("WaitTime"));
		transactions.createOrReplaceTempView("transactionsByCashier");
		transactions.show();
		Dataset<Row> averageWaitTimeByCashier = sparkSession.sql("select Date,CashierName,avg(WaitTime) as AvgWaitTime from transactionsByCashier GROUP BY Date,CashierName Order By CashierName desc");
		averageWaitTimeByCashier.show();
		writeToResultSetDB(averageWaitTimeByCashier, "averageWaitTimeByCashier");
	}

	private void dailySalesCount(SparkSession sparkSession, Dataset<Row> transactionDS) {
		Dataset<Row> transactions = transactionDS.select(substring(col("trans.trHeader.date"), 0, 10).as("Date"),col("trans.type"));
		transactions.createOrReplaceTempView("transactionCounts");
		transactions.show();
		Dataset<Row> dailySalesCount  = sparkSession.sql("select Date,count(type) as TxnCount from transactionCounts GROUP BY Date order by Date desc");
		dailySalesCount .show();
		writeToResultSetDB(dailySalesCount ,"dailySalesCount");
		
	}

	private void averageTransactionAmount(SparkSession sparkSession, Dataset<Row> transactionDS) {
		Dataset<Row> transactions = transactionDS.select(substring(col("trans.trHeader.date"), 0, 10).as("Date"),col("trans.type"),
				col("trans.trValue.trTotWTax"));
		transactions.createOrReplaceTempView("transactionTotals");
		transactions.show();
		Dataset<Row> averageTransactionAmount = sparkSession.sql("select Date,avg(trTotWTax) as AvgAmount from transactionTotals GROUP BY Date order by Date desc");
		averageTransactionAmount.show();
		writeToResultSetDB(averageTransactionAmount,"averageTransactionAmount");
		
	}

	private void salesByMOP(SparkSession sparkSession, Dataset<Row> transactionDS) {
		Dataset<Row> trPayLines = transactionDS.select(substring(col("trans.trHeader.date"), 0, 10).as("trDate"),col("trans.type"),
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
		Dataset<Row> trPayLines = transactionDS.select(substring(col("trans.trHeader.date"), 0, 10).as("trDate"),col("trans.type"),
				(explode(col("trans.trPaylines.trPayline")).as("trPaylines"))).filter(col("trPaylines.trpPayCode.value").notEqual("Change"));
		trPayLines.createOrReplaceTempView("paymentlines");
		Dataset<Row> salesByCardType = sparkSession
				.sql("SELECT trDate as Date,trPayLines.trpCardInfo.trpcCCName.value as CardType,count(trPaylines) as Count FROM paymentlines GROUP By Date,CardType Order by Date desc");
		writeToResultSetDB(salesByCardType, "salesByCardType");
		
	}

	private void salesByEntryMethod(SparkSession sparkSession, Dataset<Row> transactionDS) {
		Dataset<Row> trPayLines = transactionDS.select(substring(col("trans.trHeader.date"), 0, 10).as("trDate"),col("trans.type"),
				(explode(col("trans.trPaylines.trPayline")).as("trPaylines"))).filter(col("trPaylines.trpPayCode.value").notEqual("Change"));
		trPayLines.createOrReplaceTempView("paymentlines");
		Dataset<Row> salesByEntryMethod = sparkSession
				.sql("SELECT trDate as Date,trPayLines.trpCardInfo.trpcEntryMeth as EntryMethod,count(trPaylines) as Count FROM paymentlines GROUP By Date,EntryMethod Order by Date desc");
		salesByEntryMethod.show();
		writeToResultSetDB(salesByEntryMethod, "salesByEntryMethod");
	}

	private void discountsByLoyaltyProgram(SparkSession sparkSession, Dataset<Row> transactionDS) {
		Dataset<Row> trLines = transactionDS.select(substring(col("trans.trHeader.date"), 0, 10).as("trDate"),col("trans.type"), col("trans.trValue"),
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
