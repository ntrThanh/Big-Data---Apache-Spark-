package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.ERROR);

        SparkConf sparkConf = new SparkConf().setAppName("Application").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // tranform co ban voi spark thuc hien in ra cac chu so
        var javaRDD1 = sc.parallelize(Arrays.asList(1, 2, 7, 2, 5, 1, 3, 6, 3));
        javaRDD1.collect().forEach(x -> System.out.print(x + " "));
        System.out.println();

        // map
        var javaRDD2 = javaRDD1.map(x -> x * 2); // khi rdd1 thuc hien map,
        // no se tao ra mot rdd moi va gan gia tri rdd moi vao rdd2
        javaRDD2.collect().forEach(x -> System.out.print(x + " "));

        // filter
        var javaRDD3 = javaRDD1.filter(x -> x % 2 == 0); // lamda function thuc hien lay la
        // nhung phan tu x ma x chia het cho 2
        javaRDD3.collect().forEach(x -> System.out.print(x + " "));

        // doc du lieu tu tap tin
        var javaRDD4 = sc.textFile("C:\\Users\\Trong Thanh\\Documents" +
                "\\BIG DATA\\DemoSpark\\src\\main\\java\\org\\example\\TestDataSpark.txt");

        var javaRDD5 = javaRDD4.filter(x -> {
            String[] currentStrings = x.split(" ");
            for (int i = 0; i < currentStrings.length; i++) {
                if (currentStrings[i].equals("Spark")) {
                    return true;
                }
            }
            return false;
        }); // rdd 5 lay ra nhung dong co chua tu "Spark"

        javaRDD5.collect().forEach(x -> System.out.print(x +  " "));
        System.out.println();


        // group by key va reduce by key
        var javaRDD6 = javaRDD4.flatMap(x -> Arrays.asList(x.split(" ")).iterator()).map(x -> new Tuple2<>(x, 1));
        javaRDD6.collect().forEach(x -> System.out.print(x +  " "));
        System.out.println();

        JavaPairRDD<String, Integer> JavaPairRDD = javaRDD6.mapToPair(x -> new Tuple2<>(x._1(), x._2())).reduceByKey(Integer::sum);
        JavaPairRDD.collect().forEach(x -> System.out.print(x +  " "));
        System.out.println();


        // data frame
        // su dung dataframe de truy van ket qua diem thi trung hoc pho thong

        SparkSession ss = SparkSession.builder().appName("Application").master("local").getOrCreate();

        var dataFrame1 = ss.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("C:\\Users\\Trong Thanh\\Downloads\\diem_thi_thpt_2023.csv");
        dataFrame1.show();

        var dataFrame2 = dataFrame1.filter("toan = 8.2"); // lay ra cac thi sinh co diem toan = 8.2
        dataFrame2.show();

        var dataFrame3 = dataFrame1.filter("(toan + vat_li + hoa_hoc) > 28 and lich_su is null and dia_li is null and gdcd is null");
        dataFrame3.show(); // lay ra cac thi sinh hoc khoi A co diem thi lon hon 28

        // truy van co so du lieu su dung spark sql (spark sql chi dong vai tro la cong cu de thuc thi truy van mot cach de dang va toi uu hon)
        var df1 = getTableFromDatabase(ss, "Customers");
        df1.createOrReplaceTempView("Customers");
        df1.show(); // in ra cac thong tin trong co so du lieu cua khach hang

        var df2 = getTableFromDatabase(ss, "Orders");
        df2.createOrReplaceTempView("Orders");
        df2.show(); // in ra thong tin cac don hang

        // truy van don gian
        var df3 = ss.sql("SELECT * FROM Customers AS c INNER JOIN Orders AS o ON o.CustomerID = c.CustomerID WHERE o.OrderId = 10248");
        df3.show(); // thuc hien truy van lay ra khach hang co order id = 10248

        // truy van tinh doanh thu theo tung nam trong co so du lieu
        var df4 = getTableFromDatabase(ss, "OrderDetails");
        df4.createOrReplaceTempView("OrderDetails");
        df4.show();


        var df6 = ss.sql("select o.OrderID, sum(od.Quantity) as TotalQuantity from Orders as o inner join OrderDetails as od on o.OrderID = od.OrderID group by o.OrderID");
        df6.show();
    }

    public static Dataset<Row> getTableFromDatabase(SparkSession ss, String tableName) {
        try {
            Dataset<Row> df1 = ss.read().format("jdbc")
                    .option("url", "jdbc:mysql://localhost:3306/northwind")
                    .option("dbtable", tableName)
                    .option("user", "root")
                    .option("password", "")
                    .load();

            return df1;
        } catch (Exception e) {
            e.getCause();
        }
        return null;
    }
}