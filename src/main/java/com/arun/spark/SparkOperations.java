package com.arun.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SparkOperations {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkOperations.class);

    private static final String emails = "src/main/resources/CustomerEmails.txt";
    private static final String phones = "src/main/resources/CustomerPhoneNumbers.txt";
    private static final String transactions = "src/main/resources/CustomerTransactionDetails.txt";

    public void monthlyCustomerSpend() {
        LOGGER.info("MONTHLY CUSTOMER SPEND AGGREGATE");
        SparkConf sparkConf = new SparkConf().setAppName("MONTHLY_CUSTOMER_SPEND");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> allRecords = sparkContext.textFile(transactions);
        JavaRDD<String> records = allRecords.filter(s -> !s.startsWith("#"));
        JavaPairRDD<String, Double> customerTransactions = records.mapToPair(s -> {
            String[] lineRecord = s.split(",");
            return new Tuple2<>(String.format("%s,%s", lineRecord[0].trim(), lineRecord[1].trim()), Double.valueOf(lineRecord[2]));
        });
        JavaPairRDD<String, Double> totalCustomerSpendPerMonth = customerTransactions.reduceByKey((a,b) -> a+b).sortByKey();
        Map<String, Double> monthlyCustomerSpendMap = totalCustomerSpendPerMonth.collectAsMap();
        LOGGER.info(monthlyCustomerSpendMap.toString());
        sparkContext.stop();
    }

    public static void main(String[] args) {
        SparkOperations sparkOperations = new SparkOperations();
        sparkOperations.monthlyCustomerSpend();
    }
}