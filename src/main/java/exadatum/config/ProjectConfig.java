package exadatum.config;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;

public class ProjectConfig
{
    public static String customerRawData;
    public static String productRawData;
    public static String serverEventLogs;
    public static String appName;
    public static String master;
    public static String outputDir;

    public ProjectConfig()
    {
        HashMap<String,String> pathMap= new HashMap<String,String>();
        String fileLocationsConfig=".//Config//FileLocations.conf";
        String sparkConfig= ".//Config//Spark.conf";

        try
        {
            Scanner scanner = new Scanner(new File(fileLocationsConfig));
            while (scanner.hasNextLine())
            {
                String line = scanner.nextLine();
                pathMap.put(line.split(",")[0],line.split(",")[1]);

            }
            scanner.close();
            customerRawData=pathMap.get("customerRawData");
            productRawData=pathMap.get("productRawData");
            serverEventLogs=pathMap.get("serverEventLogs");
            outputDir=pathMap.get("outputDir");

            scanner = new Scanner(new File(sparkConfig));
            while (scanner.hasNextLine())
            {
                String line = scanner.nextLine();
                pathMap.put(line.split(",")[0],line.split(",")[1]);

            }
            scanner.close();
            appName=pathMap.get("appName");
            master=pathMap.get("master");
        }
        catch (FileNotFoundException e)
        {
            System.out.println("Please verify data in "+fileLocationsConfig);
        }
    }

    public static void main(String[] args)
    {
        ProjectConfig projectConfig = new ProjectConfig();
        System.out.println(ProjectConfig.customerRawData);
        System.out.println(ProjectConfig.productRawData);
        System.out.println(ProjectConfig.serverEventLogs);
        System.out.println(ProjectConfig.appName);
        System.out.println(ProjectConfig.master);
        System.out.println(ProjectConfig.outputDir);
    }
}
