import exadatum.Products.ProductData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import exadatum.customers.CustomerData;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.storage.StorageLevel;
import exadatum.config.ProjectConfig;
import exadatum.ServerLogs.ServerLogData;

//search for main method in ProcessData class


public class ProcessData
{

    public static void createAndSaveCatalogues(SparkSession sparkSession,Dataset<Row> customerData,Dataset<Row> productsData,ProjectConfig projectConfig)
    {
        ServerLogData serverLogData = new ServerLogData(sparkSession);
        Dataset<Row> purchaseEventLogs = serverLogData.purchaseEventLogs;
        Dataset<Row> addToCartEventLogs = serverLogData.addToCartEventLogs;
        Dataset<Row> productViewEventLogs = serverLogData.productViewEventLogs;

        //Create Catalogues for all events
        Dataset<Row> purchaseCatalogue= purchaseEventLogs.select("CustomerId","ProductId","Timestamp","Channel").join(customerData,"CustomerId").select("CustomerId","ProductId","Timestamp","City","State","ZipCode","Channel").join(productsData,"ProductId").select("CustomerId","ProductId","Timestamp","City","State","ZipCode","Product","Category","SubCategory","Channel");
        Dataset<Row> addToCartCatalogue= addToCartEventLogs.select("CustomerId","ProductId","Timestamp","Channel").join(customerData,"CustomerId").select("CustomerId","ProductId","Timestamp","City","State","ZipCode","Channel").join(productsData,"ProductId").select("CustomerId","ProductId","Timestamp","City","State","ZipCode","Product","Category","SubCategory","Channel");
        Dataset<Row> productViewCatalogue= productViewEventLogs.select("CustomerId","ProductId","Timestamp","Channel").join(customerData,"CustomerId").select("CustomerId","ProductId","Timestamp","City","State","ZipCode","Channel").join(productsData,"ProductId").select("CustomerId","ProductId","Timestamp","City","State","ZipCode","Product","Category","SubCategory","Channel");

        //Save catalogues partitioned on Timestamp date and hour.
        productViewCatalogue.write().mode(SaveMode.Overwrite).format("csv").option("header",true).option("delimiter",",").partitionBy("Timestamp").save(projectConfig.outputDir+"ProductViewCatalogue");
        addToCartCatalogue.write().mode(SaveMode.Overwrite).format("csv").option("header",true).option("delimiter",",").partitionBy("Timestamp").save(projectConfig.outputDir+"AddToCartCatalogue");
        purchaseCatalogue.write().mode(SaveMode.Overwrite).format("csv").option("header",true).option("delimiter",",").partitionBy("Timestamp").save(projectConfig.outputDir+"PurchaseCatalogue");
    }

    public static void extractData(SparkSession sparkSession)
    {
        //top 5 selling products for given Channel=Mobile
        Dataset<Row> top5ProductsChannel = sparkSession.sql("SELECT ProductId,Product,Channel,COUNT(*) as Count FROM PurchaseCatalogue WHERE Channel='Mobile' GROUP BY ProductId,Product,Channel ORDER BY Count DESC limit 5");
        top5ProductsChannel.show();

        //top 5 selling products in Category Electronics
        Dataset<Row> top5ProductsCategory = sparkSession.sql("SELECT ProductId,Product,Category,COUNT(*) as Count FROM PurchaseCatalogue WHERE Category='Electronics' GROUP BY ProductId,Product,Category ORDER BY Count DESC limit 5");
        top5ProductsCategory.show();

        //total sold products by Zipcode,City,State
        Dataset<Row> cityProductsSoldCount = sparkSession.sql("SELECT City,State,Zipcode,COUNT(*) as Count FROM PurchaseCatalogue GROUP BY City,State,Zipcode,Timestamp ORDER BY Count DESC");
        cityProductsSoldCount.show();

    }

    public static void createHiveTableProductsSoldPerCity(SparkSession sparkSession)
    {
        Dataset<Row> cityProductsSoldCount = sparkSession.sql("SELECT City,State,Zipcode,Count(*) as Count,Timestamp as TimestampDateHr FROM PurchaseCatalogue GROUP BY City,State,Zipcode,Timestamp");
        //cityProductsSoldCount.write().option("header",true).partitionBy("TimestampDateHr").saveAsTable("CityProductsSoldCount");
        cityProductsSoldCount.write().option("header",true).mode(SaveMode.Overwrite).insertInto("ProductsSoldPerCity");
        //cityProductsSoldCount.show();
    }

    public static void main(String[] args)
    {
        ProjectConfig projectConfig= new ProjectConfig();   //Initialize ProjectConfig class. It will read configurations from external text file. projectConfig will contaion spark configurations such as Master and AppName and input file locations such as Customers.csv 
        SparkConf sparkConf = new SparkConf().setAppName(projectConfig.appName).setMaster(projectConfig.master);
        SparkSession sparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate();
        sparkSession.sql("set hive.exec.dynamic.partition.mode=nonstrict");                     
        sparkSession.sql("set hive.exec.dynamic.partition=true");

        Dataset<Row> customerData = new CustomerData(sparkSession).mapAndGetCustomers();                                //fetch Customer table in customerData dataset
        Dataset<Row> productsData = new ProductData(sparkSession).getProducts();                                        //fetch Products table in productsData dataset
        customerData.show();
        productsData.show();

        //customerData.select("CustomerId","FirstName","ZipCode","State").filter("ZipCode == 070116").show();          //This is how u perform filter
        //customerData.select("State").groupBy("state").count().show();                                                //this is how you perform Group By
        //productsData.select("Category").groupBy("Category").count().show();                                          //Get category wise count


        createAndSaveCatalogues(sparkSession,customerData,productsData,projectConfig);   //This function will create Catalogues. ProductViewCatalogue, AddToCartCatalogue, PurchaseCatalogue and saves these catalogues in .//Output directory partitioned on Timestamp (Timestamp has been mapped to contain only date and hr information. Minutes and seconds have been removed in mapping)

        Dataset<Row> purchaseCatalogue = sparkSession.read().option("header",true).option("delimiter",",").csv(projectConfig.outputDir+"PurchaseCatalogue");        //Load data from purchase catalogue. This catalogue data will be used to extract insights such as Top 5 selling products
        purchaseCatalogue.createOrReplaceTempView("PurchaseCatalogue");               //create temporory view on Purchase Catalogue so we can perform SQL queries on it.
        purchaseCatalogue.show();

        extractData(sparkSession);                                      //This method will query the required data in Probelm statement. I was little confused about the required data whether to create Hive tables or Dataframes as output. So I created Dataframes in this method as result of query. In next method I created hive tables.
        createHiveTableProductsSoldPerCity(sparkSession);               //In this method I inserted data in partitioned hive table for ProductsSoldPerCity table. This table is already created in hive as [CREATE TABLE ProductsSoldPerCity (City string,State string,Zipcode string,Count int)PARTITIONED BY(TimestampDateHr string);] So it is partitioned on column TimestampDateHr
    }

}
