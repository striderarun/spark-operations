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

    public void aggregateCustomerEmailsAndPhoneNumbers() {
        LOGGER.info("AGGREGATE CUSTOMER EMAILS AND PHONES");
        SparkConf sparkConf = new SparkConf().setAppName("AGGREGATE_CUSTOMER_DETAILS");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> userEmailLines = sparkContext.textFile(emails);
        JavaRDD<String> userPhoneLines = sparkContext.textFile(phones);

        JavaPairRDD<String, String> userEmails = userEmailLines.mapToPair(s -> {
            String[] val = s.split(",");
            return new Tuple2<>(val[0].trim(), val[1].trim());
        });

        JavaPairRDD<String, String> userPhones = userPhoneLines.mapToPair(s -> {
            String[] val = s.split(",");
            return new Tuple2<>(val[0].trim(), val[1].trim());
        });

        JavaPairRDD<String, Iterable<String>> userEmailGroups = userEmails.groupByKey();
        JavaPairRDD<String, Iterable<String>> userPhoneGroups = userPhones.groupByKey();

        Map<String, Tuple2<Iterable<String>, Iterable<String>>> userJoinMap = userEmailGroups.join(userPhoneGroups).sortByKey().collectAsMap();

        LOGGER.info(userJoinMap.toString());
        sparkContext.stop();

    }

    public void coGroupCustomerEmailsAndPhoneNumbers() {
        System.out.println("CO-GROUP CUSTOMER EMAILS AND PHONES");
        SparkConf sparkConf = new SparkConf().setAppName("COGROUP_CUSTOMER_DETAILS");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> userEmailLines = sparkContext.textFile(emails);
        JavaRDD<String> userPhoneLines = sparkContext.textFile(phones);

        JavaPairRDD<String, String> userEmails = userEmailLines.mapToPair(s -> {
            String[] val = s.split(",");
            return new Tuple2<>(val[0].trim(), val[1].trim());
        });

        JavaPairRDD<String, String> userPhones = userPhoneLines.mapToPair(s -> {
            String[] val = s.split(",");
            return new Tuple2<>(val[0].trim(), val[1].trim());
        });

        Map<String, Tuple2<Iterable<String>, Iterable<String>>> coGroupMap = userEmails.cogroup(userPhones).sortByKey().collectAsMap();

        LOGGER.info(coGroupMap.toString());
        sparkContext.stop();

    }

    public static void main(String[] args) {
        SparkOperations sparkOperations = new SparkOperations();
        sparkOperations.monthlyCustomerSpend();
        sparkOperations.aggregateCustomerEmailsAndPhoneNumbers();
        sparkOperations.coGroupCustomerEmailsAndPhoneNumbers();
    }
}