package exadatum.customers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import exadatum.config.ProjectConfig;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.io.Serializable;
import java.util.Arrays;


public class CustomerData implements Serializable
{
    private SparkSession sparkSession;
    public CustomerData(SparkSession sparkSession)
    {
        this.sparkSession = sparkSession;
    }


    private StructType createCustomerSchema()
    {
        StructField customerId = DataTypes.createStructField("CustomerId",DataTypes.StringType,true);
        StructField firstName = DataTypes.createStructField("FirstName",DataTypes.StringType,true);
        StructField lastName = DataTypes.createStructField("LastName",DataTypes.StringType,true);
        StructField city = DataTypes.createStructField("City",DataTypes.StringType,true);
        StructField country = DataTypes.createStructField("Country",DataTypes.StringType,true);
        StructField state = DataTypes.createStructField("State",DataTypes.StringType,true);
        StructField zipCode = DataTypes.createStructField("ZipCode",DataTypes.StringType,true);
        StructType rowSchema = DataTypes.createStructType(Arrays.asList(customerId,firstName,lastName,city,country,state,zipCode));
        return rowSchema;
    }

    public Dataset<Row> mapAndGetCustomers()
    {
        Dataset<Row> customerData = sparkSession.read().option("header",true).csv(ProjectConfig.customerRawData); //read customer data

        StructType customerSchema = createCustomerSchema();                     //define schema for Customer table

        MapFunction <Row, Row> mapZipcode = new MapFunction<Row, Row>()         //This function is used to map zipcode to 6 characters
        {
            @Override
            public Row call(Row row) throws Exception {

                String zipCode = row.getAs("Zip");
                while (zipCode.length()<6)
                {
                    zipCode="0"+zipCode;
                }
                return RowFactory.create(row.getAs("CustomerId"),row.getAs("First_Name"),row.getAs("Last_Name"),row.getAs("City"),row.getAs("Country"),row.getAs("State"),zipCode);
            }
        };

        Dataset<Row> mappedZipCustomers= customerData.map(mapZipcode,RowEncoder.apply(customerSchema));
        return mappedZipCustomers;
    }

    public static void main(String[] args)
    {
        SparkConf sparkConf = new SparkConf().setAppName("Exadatum_CustomerData").setMaster("local");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        CustomerData data = new CustomerData(sparkSession);
        Dataset<Row> customerData = data.mapAndGetCustomers();
        customerData.show();

    }
}
