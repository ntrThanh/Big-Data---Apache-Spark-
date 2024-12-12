<h1>
Thực hành demo cách sử dụng, cài đặt Apache Spark 
</h1>
<h2>Nội dung thực hành:</h2>
  <ul>
      <li>Thực hành cài đặt, sử dụng công cụ Apache Spark bằng Java trên Intellij IDEA, Command Prompt.</li>
      <li>Thực hành khởi tạo và thao tác với Spark RDD.</li>
      <li>Thực hành khởi tạo và thao tác với Spark Data Frame để truy vấn dữ liệu.</li>
      <li>Thực hành sử dụng Spark SQL để thao tác và truy vấn dữ liệu trên CSDL MySQL.</li>
   
  </ul>

<h2>
  Thực hành 
</h2>

<h3> 
  Cài đặt Spark  
</h3>

<p>
  Do bị giới hạn về phần cứng nên ở đây chúng tôi xin trình bày cách cài đặt Apache Spark trên chế độ Local mode (sử dụng Google colab thì trong một phiên chỉ sử dụng tối đa 1 CPU nên việc cài đặt cluster trên Google colab gần như không khả thi).
</p>

<span>Các bước cài đặt trên trực tiếp hệ điều hành Window</span>

<ul>
  <li>Cài đặt phiên bản JDK 8 / 11 / 17 (hoặc có thể cài đặt Python, R phiên bản mới nhất nếu sử dụng PySpark, RSpark ...), một số phiên bản khác sẽ khiến trương trình không chạy được.</li>
  <li>
  Cài đặt Winutil. 
  </li>
  <li>
  Thiết lập biến môi trường cho các công cụ trên.
  </li>
  <li>
  Kiểm tra lại bằng cách mở CMD và gõ:

    spark-shell

  </li>
</ul>

<span>Các bước cài đặt bằng chương trình Java sử dụng Intellij IDEA</span>

<ul>
  <li>
    Tạo một Project mới với Maven.
  </li>
  <li>
  Sử dụng JDK 8 / 11 / 17 cho chương trình Java.
  </li>
  <li>
  Vào phần cài đặt của IntelliJ -> Chọn Plugins -> Download Scala. 
  </li>
  <li>
  Vào trang web <a href="https://mvnrepository.com/"> maven repository</a> và tìm kiểm thư viện Apache Spark là Spark 
Project Core sau đó thêm vào file <code>pom.xml</code> trong thẻ <code>dependencies</code>.

    <!-- https://mvnrepository.com/artifact/org.apache.spark/spark-core -->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_2.12</artifactId>
        <version>3.4.0</version>
    </dependency>

  Còn phần sau đây để sử dụng trong Spark SQL:

    <!-- https://mvnrepository.com/artifact/org.apache.spark/spark-sql -->
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_2.12</artifactId>
        <version>3.4.0</version>
    </dependency>

    <!-- https://mvnrepository.com/artifact/log4j/log4j -->
    <dependency>
        <groupId>log4j</groupId>
        <artifactId>log4j</artifactId>
        <version>1.2.12</version>
    </dependency>

    <!-- https://mvnrepository.com/artifact/mysql/mysql-connector-java -->
    <dependency>
        <groupId>mysql</groupId>
        <artifactId>mysql-connector-java</artifactId>
        <version>8.0.33</version>
    </dependency>

  </li>

  <li>
  Sau đó thể phần build vào file <code>pom.xml</code>.
    
    <build>
        <plugins>
            <plugin>
                <groupId>org.codehaus.mojo</groupId>
                <artifactId>build-helper-maven-plugin</artifactId>
                <version>3.6.0</version>
                <executions>
                    <execution>
                        <id>add-source</id>
                        <phase>generate-sources</phase>
                        <goals>
                            <goal>add-source</goal>
                        </goals>
                        <configuration>
                            <sources>
                                <source>src/main/java</source>
                            </sources>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
  </li>

  <li>
  Chạy trương trình đầu tiên với Spark:

    package org.example;

    import java.util.*;

    import org.apache.spark.api.java.JavaSparkContext;
    import org.apache.spark.api.java.JavaRDD;
    import org.apache.spark.SparkConf;

    public class Main {
        public static void main(String[] args) {

            SparkConf conf = new SparkConf().setAppName("application").setMaster("local");
            JavaSparkContext sc = new JavaSparkContext(conf);

            List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
            JavaRDD<Integer> distData = sc.parallelize(data);

            JavaRDD<Integer> newData = distData.map(x -> x * 2);
        }
    }

  </li>

</ul>

<h3> 
  Thực hành Demo
</h3>

<p>
  Ở đây chúng tôi Demo bằng chương trình Java để thao tác với dữ liệu trên Intelij IDEA.
</p>

<h4>
  Làm việc với RDD
</h4>

<p>Khời tạo Spark Context:

    SparkConf sparkConf = new SparkConf().setAppName("Application").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

</p>

<p> Các cách khởi tạo RDD để thao tác với dữ liệu:
  <br>Khởi tạo song song:

    var javaRDD1 = sc.parallelize(Arrays.asList(1, 2, 7, 2, 5, 1, 3, 6, 3));
    javaRDD1.collect().forEach(x -> System.out.print(x + " "));
    System.out.println();

  <br>
  
    Kết quả: 
    1 2 7 2 5 1 3 6 3

  <br> Khởi tạo từ một RDD khác:

    // map
    var javaRDD2 = javaRDD1.map(x -> x * 2); // khi rdd1 thuc hien map,
    // no se tao ra mot rdd moi va gan gia tri rdd moi vao rdd2
    javaRDD2.collect().forEach(x -> System.out.print(x + " ")); 

    // filter
    var javaRDD3 = javaRDD1.filter(x -> x % 2 == 0); // lamda function thuc hien lay la
    // nhung phan tu x ma x chia het cho 2
    javaRDD3.collect().forEach(x -> System.out.print(x + " "));

  <br>

    Kết quả: 
    2 4 14 4 10 2 6 12 6
    2 2 6

  <br>Khởi tạo từ dữ liệu của file:


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

  <br>

    Kết quả: 
    ve chu de Apache Spark 


  <br> Các phương pháp <code>group by key, reduce by key</code> trong RDD 

    // group by key va reduce by key
    var javaRDD6 = javaRDD4.flatMap(x -> Arrays.asList(x.split(" ")).iterator()).map(x -> new Tuple2<>(x, 1));
    javaRDD6.collect().forEach(x -> System.out.print(x +  " "));
    System.out.println();

    JavaPairRDD<String, Integer> JavaPairRDD = javaRDD6.mapToPair(x -> new Tuple2<>(x._1(), x._2())).reduceByKey(Integer::sum);
    JavaPairRDD.collect().forEach(x -> System.out.print(x +  " "));
    System.out.println();

  <br>

    Kết quả: 
    (xin,1) (chao,1) (moi,1) (nguoi,1) (hom,1) (nay,1) (toi,1) (thuc,1) (hanh,1) (demo,1) (ve,1) (du,1) (lieu,1) (lon,1) (ve,1) (chu,1) (de,1) (Apache,1) (Spark,1) (moi,1) (thac,1) (mac,1) (lien,1) (he,1) (truc,1) (tiep,1) (toi,1) (toi,1) (qua,1) (email,1) (nguyentrongthanh672@gmail.com,1) 
    (hanh,1) (hom,1) (Spark,1) (nay,1) (de,1) (qua,1) (Apache,1) (lien,1) (ve,2) (demo,1) (nguoi,1) (he,1) (mac,1) (toi,3) (truc,1) (chao,1) (du,1) (email,1) (nguyentrongthanh672@gmail.com,1) (lon,1) (xin,1) (chu,1) (moi,2) (thuc,1) (tiep,1) (lieu,1) (thac,1) 


</p>

<h4>
  Làm việc với Dataframe
</h4>

<p>
  Khởi tạo SparkSession:

    SparkSession ss = SparkSession.builder().appName("Application").master("local").getOrCreate();

</p>

<p>
  Thao tác dữ liệu với Dataframe:

    var dataFrame1 = ss.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("C:\\Users\\Trong Thanh\\Downloads\\diem_thi_thpt_2023.csv");
    dataFrame1.show();

    var dataFrame2 = dataFrame1.filter("toan = 8.2"); // lay ra cac thi sinh co diem toan = 8.2
    dataFrame2.show();

    var dataFrame3 = dataFrame1.filter("(toan + vat_li + hoa_hoc) > 28 and lich_su is null and dia_li is null and gdcd is null");
    dataFrame3.show(); // lay ra cac thi sinh hoc khoi A co diem thi lon hon 28
</p>

<h4>
  Làm việc với Spark SQL
</h4>

<p>
  Có thể sử dụng cơ sở dữ liệu trong các hệ quản trị cơ sở dữ liệu. Ở đây tôi sử dụng với MySQL thông qua JDBC.

  Thêm thư viện Spark SQL vào trong file <code>pom.xml</code> cùng với đó là thư viện JDBC của Java (trên tiêu đề đã có).

Hàm lấy ra những bảng cơ sở dữ liệu dưới dạng <code>Dataset&lt;Row></code>:

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

Truy vấn đơn giản, trước khi truy vấn với bảng thì phải tạo view:

    // truy van co so du lieu su dung spark sql (spark sql chi dong vai tro la cong cu de thuc thi truy van mot cach de dang va toi uu hon)
    var df1 = getTableFromDatabase(ss, "Customers");
    df1.createOrReplaceTempView("Customers");
    df1.show(); // in ra cac thong tin trong co so du lieu cua khach hang

    var df2 = getTableFromDatabase(ss, "Orders");
    df2.createOrReplaceTempView("Orders");
    df2.show(); // in ra thong tin cac don hang

    // truy van don gian
    Dataset<Row> df3 = ss.sql("SELECT * FROM Customers AS c INNER JOIN Orders AS o ON o.CustomerID = c.CustomerID WHERE o.OrderId = 10248");
    df3.show(); // thuc hien truy van lay ra khach hang co order id = 10248

    // truy van tinh doanh thu theo tung nam trong co so du lieu
    var df4 = getTableFromDatabase(ss, "OrderDetails");
    df4.createOrReplaceTempView("OrderDetails");
    df4.show();


    var df6 = ss.sql("select o.OrderID, sum(od.Quantity) as TotalQuantity " +
            "from Orders as o " +
            "inner join OrderDetails as od " +
            "on o.OrderID = od.OrderID " +
            "group by o.OrderID");
    df6.show();

</p>

<h2>
Note 
</h2>

<p>
  Tại thời điểm chạy trương trình, có thể sử dụng luồng (Thread) trong Java để có thể ngăn cách các trương trình chạy, sau đó truy cập localhost của chương trình trên trình duyệt để theo dõi cách chương trình hoạt động (việc phân vùng, chia các task, stage).

    public class Main throws InterruptedException {
      public static void main(String[] ags) {
        // code here


        Thread.sleep(60000); // 60000 mili giây
      }
    }

  Ở đây chúng tôi lấy ví dụ đặt ở cuối trương trình để có thể xem được toàn bộ các hoạt động trong chương trình của Apache Spark.
  Ví dụ như: <code>http://DESKTOP-3K5JCE6:4040</code>, tùy thuộc vào mỗi máy lại có các địa chỉ localhost của chương trình khác nhau.

  <img src="/img/Spark.png" alt="Spark Logo" /></p>

<h2>
Nguồn dữ liệu:
</h2>
<ul>
  <li>
  Cơ sở dữ liệu <a href="https://support.microsoft.com/vi-vn/office/s%C6%A1-%C4%91%E1%BB%93-c%C6%A1-s%E1%BB%9F-d%E1%BB%AF-li%E1%BB%87u-northwind-cd422d47-e4e3-4819-8100-cdae6aaa0857">Northwind </a>
  </li>

  <li>
  Dữ liệu <a href="https://github.com/anhdung98/diem_thi_2023">điểm thi trung học phổ thông</a>
  </li>

</ul>

<h2>
Tham khảo:
</h2>

<ul>
  <li>
  Tham khảo tại <a href="https://spark.apache.org/docs/3.5.3/">Spark Documentation</a>
  </li>

</ul>
