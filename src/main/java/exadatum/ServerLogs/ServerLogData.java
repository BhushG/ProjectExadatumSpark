package exadatum.ServerLogs;

import exadatum.config.ProjectConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.io.Serializable;
import java.util.Arrays;

import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.floor;


//This ChannelPriceTimestampMapper will add new Column Channel based on source, modifies Price to float type and converts timestamp to contain only date and hr information
class ChannelPriceTimeStampMapper implements Serializable, MapFunction<Row,Row>
{
    //Only the Purchase schema contains Quantity column,
    //ProductView and AddToCart schema are same

    boolean quantityPresent;
    ChannelPriceTimeStampMapper(boolean quantityPresent)
    {
        this.quantityPresent= quantityPresent;
    }

    public Row call(Row row) throws Exception
    {
        //Process TimeDateStamp
        String timeStamp= row.getAs("timestamp");
        String date= timeStamp.split(" ")[0];
        String hour= timeStamp.split(" ")[1].split(":")[0];
        timeStamp=date+":"+hour;
        timeStamp=timeStamp.replaceAll(" ","");
        timeStamp=timeStamp.replaceAll(":","-");

        //Process Source
        String source= row.getAs("Source");
        String channel = "Mobile";
        if(source.contains("desk"))
            channel="Desk";
        else if(source.contains("tablet"))
            channel="Tablet";


        //Process Price
        String price= row.getAs("Price");
        float floatPrice= 0.0f;
        if(price.endsWith("$"))
            price=price.substring(0,price.length()-1);

        try
        {
            floatPrice= Float.parseFloat(price);
        }
        catch (Exception exception)
        {
            //String to float conversion exception, Ignore it.
        }
        if(quantityPresent)
            return RowFactory.create(timeStamp,row.getAs("CustomerId"),row.getAs("ProductId"),floatPrice,row.getAs("Source"),channel,row.getAs("Quantity"));
        return RowFactory.create(timeStamp,row.getAs("CustomerId"),row.getAs("ProductId"),floatPrice,row.getAs("Source"),channel);
    }
}

//This class is used to filter Dataset Rows according to event type. It creates filter for Event in argument and returns dataset after applying that filter.
class FilterServerLogEvents implements Serializable,FilterFunction<Row>
{
    String event;
    public FilterServerLogEvents(String event)
    {
        this.event=event;
    }

    @Override
    public boolean call(Row row)
    {
        GenericRowWithSchema grs= (GenericRowWithSchema)row.getAs("EventType");
        return (grs.getAs(event) != null);
    }

}

public class ServerLogData implements Serializable
{
    private SparkSession sparkSession;
    public Dataset<Row> purchaseEventLogs;
    public Dataset<Row> addToCartEventLogs;
    public Dataset<Row> productViewEventLogs;

    public ServerLogData(SparkSession sparkSession)
    {
        this.sparkSession=sparkSession;
        mapAndGetServerLogs();
    }


    private StructType productViewAndAddToCartSchema()   //ProductView dataset and AddToCart dataset, both have same schema
    {
        StructField timestamp = DataTypes.createStructField("Timestamp",DataTypes.StringType,true);
        StructField customerId = DataTypes.createStructField("CustomerId",DataTypes.StringType,true);
        StructField productId = DataTypes.createStructField("ProductId",DataTypes.StringType,true);
        StructField price = DataTypes.createStructField("Price",DataTypes.FloatType,true);
        StructField source = DataTypes.createStructField("Source",DataTypes.StringType,true);
        StructField channel = DataTypes.createStructField("Channel",DataTypes.StringType,true);
        StructType rowSchema = DataTypes.createStructType(Arrays.asList(timestamp,customerId,productId,price,source,channel));
        return rowSchema;
    }

    private StructType purchaseProductSchema()   //Purchase dataset has one more column i.e. Quantity 
    {
        StructField timestamp = DataTypes.createStructField("Timestamp",DataTypes.StringType,true);
        StructField customerId = DataTypes.createStructField("CustomerId",DataTypes.StringType,true);
        StructField productId = DataTypes.createStructField("ProductId",DataTypes.StringType,true);
        StructField price = DataTypes.createStructField("Price",DataTypes.FloatType,true);
        StructField source = DataTypes.createStructField("Source",DataTypes.StringType,true);
        StructField channel = DataTypes.createStructField("Channel",DataTypes.StringType,true);
        StructField quantity = DataTypes.createStructField("Quantity",DataTypes.LongType,true);
        StructType rowSchema = DataTypes.createStructType(Arrays.asList(timestamp,customerId,productId,price,source,channel,quantity));
        return rowSchema;
    }



    public Dataset<Row> mapAndGetServerLogs()
    {
        Dataset<Row> serverLogs = sparkSession.read().json(ProjectConfig.serverEventLogs);  //we are going to perform multiple actions on this Raw data so persist it in memory.
        Dataset<Row> purchaseEventLogs= serverLogs.filter(new FilterServerLogEvents("Purchase"));
        Dataset<Row> addToCartEventLogs= serverLogs.filter(new FilterServerLogEvents("AddToCart"));
        Dataset<Row> productViewEventLogs= serverLogs.filter(new FilterServerLogEvents("ProductView"));


        //Add columns ProductId , Price from EventType.AddToCart and delete EventType
        addToCartEventLogs=addToCartEventLogs.withColumn("ProductId",addToCartEventLogs.col("EventType.AddToCart.ProductId"));
        addToCartEventLogs=addToCartEventLogs.withColumn("Price",addToCartEventLogs.col("EventType.AddToCart.Price"));
        addToCartEventLogs=addToCartEventLogs.drop("EventType");

        //Add columns ProductId , Price from EventType.ProductView and delete EventType
        productViewEventLogs=productViewEventLogs.withColumn("ProductId",productViewEventLogs.col("EventType.ProductView.ProductId"));
        productViewEventLogs=productViewEventLogs.withColumn("Price",productViewEventLogs.col("EventType.ProductView.Price"));
        productViewEventLogs=productViewEventLogs.drop("EventType");


        //The purchased products in Purchase event are stored in WrappedArray, we need to use explode function to unwrap it.
        purchaseEventLogs=purchaseEventLogs.withColumn("Products",explode(purchaseEventLogs.col("EventType.Purchase"))).drop("EventType");
        purchaseEventLogs=purchaseEventLogs.withColumn("ProductId",purchaseEventLogs.col("Products.ProductId"));
        purchaseEventLogs=purchaseEventLogs.withColumn("Price",purchaseEventLogs.col("Products.Price"));
        purchaseEventLogs=purchaseEventLogs.withColumn("Quantity",purchaseEventLogs.col("Products.Quantity"));
        purchaseEventLogs=purchaseEventLogs.drop("Products");


        //ChannelPriceTimestampMapper will add new Column Channel based on source, modifies Price to float type and converts timestamp to contain only date and hr information
        purchaseEventLogs=purchaseEventLogs.map(new ChannelPriceTimeStampMapper(true),RowEncoder.apply(purchaseProductSchema()));
        addToCartEventLogs=addToCartEventLogs.map(new ChannelPriceTimeStampMapper(false),RowEncoder.apply(productViewAndAddToCartSchema()));
        productViewEventLogs=productViewEventLogs.map(new ChannelPriceTimeStampMapper(false),RowEncoder.apply(productViewAndAddToCartSchema()));

        this.purchaseEventLogs= purchaseEventLogs;
        this.addToCartEventLogs= addToCartEventLogs;
        this.productViewEventLogs= productViewEventLogs;
        return productViewEventLogs;
    }

    public static void main(String[] args)
    {
        SparkConf sparkConf = new SparkConf().setAppName("Exadatum_ServerData").setMaster("local");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        ServerLogData serverLogData = new ServerLogData(sparkSession);
        serverLogData.mapAndGetServerLogs().show();
    }
}
