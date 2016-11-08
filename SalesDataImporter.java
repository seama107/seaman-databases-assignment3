import java.io.FileReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;


public class SalesDataImporter {
  private File dataCSVFile;
  private BufferedReader dataReader;
  private SqlConnectionStream sqlCon;

  public SalesDataImporter(File dat) {
    //I'm in the middle of adding the SQl connection Stream
    //as to a part of this class and then testing it with the printDatabaseSchema
    //method

    //dont forget I need to check for sqlCon.hasDriver()
    sqlCon = new SqlConnectionStream();
    dataCSVFile = dat;
    try {
      dataReader = new BufferedReader(new FileReader(dataCSVFile));
    }
    catch(IOException e) {
      System.out.println("Error opening file:");
      e.printStackTrace();
    }
  }

  public SalesDataImporter() {
    this(new File("salesData.csv"));
  }


  public void processTuple(String[] tuple){

  }

  public String[] readTuple() throws Exception {
    return dataReader.readLine().split(",");
  }

  public void createTables() {
    //Not to be used in the application, just for the inital
    //creation of the schema
    String createCustomersStatement = "CREATE TABLE railCoCustomers(" +
  "customer_id INT UNSIGNED PRIMARY KEY NOT NULL AUTO_INCREMENT," +
  "first_name VARCHAR(25)," +
  "last_name VARCHAR(25));";

    String createCustomerPhonesStatement = "CREATE TABLE railCoCustomerPhones(" +
  "phone_id INT UNSIGNED PRIMARY KEY NOT NULL AUTO_INCREMENT," +
  "customer_id INT UNSIGNED," +
  "phone_number VARCHAR(30)," +
  "type VARCHAR(4)," +
  "FOREIGN KEY (customer_id) REFERENCES railCoCustomers(customer_id));";

    String createStoresStatement = "CREATE TABLE railCoStores(" +
  "store_id INT UNSIGNED PRIMARY KEY NOT NULL AUTO_INCREMENT," +
  "street_address VARCHAR(50)," +
  "zipcode INT UNSIGNED," +
  "city VARCHAR(20)," +
  "state VARCHAR(2));";

    String createItemsStatement = "CREATE TABLE railCoItems(" +
  "item_id INT UNSIGNED PRIMARY KEY NOT NULL," +
  "item_name VARCHAR(50)," +
  "item_manf VARCHAR(50)," +
  "price DECIMAL(8,2));";

    String createTransactionsStatement = "CREATE TABLE railCoTransactions(" +
  "transaction_id INT UNSIGNED PRIMARY KEY NOT NULL AUTO_INCREMENT," +
  "item_id INT UNSIGNED," +
  "customer_id INT UNSIGNED," +
  "store_id INT UNSIGNED," +
  "timestamp DATETIME," +
  "FOREIGN KEY (item_id) REFERENCES railCoItems(item_id)," +
  "FOREIGN KEY (customer_id) REFERENCES railCoCustomers(customer_id)," +
  "FOREIGN KEY (store_id) REFERENCES railCoStores(store_id));";


    sqlCon.createTable(createCustomersStatement);
    sqlCon.createTable(createCustomerPhonesStatement);
    sqlCon.createTable(createStoresStatement);
    sqlCon.createTable(createItemsStatement);
    sqlCon.createTable(createTransactionsStatement);
  }


  public void printDatabaseSchema() {
    sqlCon.printDatabaseSchema();
  }

  public void closeDataReader() throws IOException {
    dataReader.close();
  }

  public boolean hasSQLDriver() {
    return sqlCon.hasDriver();
  }

  public void quitelyClose() {
    try {
      dataReader.close();
    }
    catch(Exception e) {
      System.out.println("Error closing data file:");
      e.printStackTrace();
    }


  }

}
