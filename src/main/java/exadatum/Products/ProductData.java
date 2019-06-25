package exadatum.Products;
import exadatum.config.ProjectConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ProductData
{
    private SparkSession sparkSession;
    public ProductData(SparkSession sparkSession)
    {
        this.sparkSession= sparkSession;
    }

    public Dataset<Row> getProducts()
    {
        Dataset<Row> productData = sparkSession.read().option("header",true).csv(ProjectConfig.productRawData); //read Product data
        return productData;
    }

    public static void main(String[] args)
    {
        SparkConf sparkConf = new SparkConf().setAppName("Exadatum_CustomerData").setMaster("local");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        ProductData productData = new ProductData(sparkSession);
        productData.getProducts().show();
    }
}
