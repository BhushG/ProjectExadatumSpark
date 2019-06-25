# ProjectExadatumSpark
* Code is Present in src/main/java
* RawData folder contains Products.csv and Customers.csv files which contains information about Products and Customers respectively.
* ProcessData class contains main method of this Project. It creates sparksession.
* exadatum.config.ProjectConfig class is used to configure project settings. It reads required data like Input file locations and spark configurations from files in ./Config folder
* exadatum.customers.CustomerData class reads data from "Customers.csv" and applies required transformations and creates Customers dataframe.
* exadatum.Products.ProductData class reads data from "Products.csv" file and creates Products dataframe.
* exadatum.ServerLogs.ServerLogData class processes the Event Logs for events AddToCart, Purchase, ProductView and creates Dataframes by appying required transformation.
