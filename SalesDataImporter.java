import java.io.FileReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;


public class SalesDataImporter {
  private File dataCSVFile;
  private BufferedReader dataReader;
  private RailCoConnectionStream sqlCon;

  public SalesDataImporter(File dat) {
    //I'm in the middle of adding the SQl connection Stream
    //as to a part of this class and then testing it with the printDatabaseSchema
    //method

    //dont forget I need to check for sqlCon.hasDriver()
    sqlCon = new RailCoConnectionStream();
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

  public void importUnstructuredData() {
    try {
      processTuple(readTuple());
    }
    catch (Exception e) {
      e.printStackTrace();
    }

  }


  public void processTuple(String[] tuple){
    /*
    timestamp,address,zip,city,state,item_id,item_name,item_manf,price,first,last,home,cell
    */
    int zip = 11111;
    double price = 0.0;
    int item_id = 1;
    try {
      zip = Integer.parseInt(tuple[2]);
      System.out.println(String.format("zip: %d", zip));
      price = Double.parseDouble(tuple[8]);
      System.out.println(String.format("price: %f", price));
      item_id = Integer.parseInt(tuple[5]);
      System.out.println(String.format("item_id: %d", item_id));
    }
    catch(Exception e) {
      e.printStackTrace();
    }
    insertCustomer(tuple[9], tuple[10]);
    insertStore(tuple[1], zip, tuple[3], tuple[4]);
    insertCustomerPhone(1, tuple[12], "cell");
    insertItem(item_id, tuple[6], tuple[7], price);
    insertTransaction(1,1,1, Timestamp.valueOf(tuple[0]) );
  }

  public String[] readTuple() throws Exception {
    return dataReader.readLine().split(",");
  }

  public void insertCustomer(String firstName, String lastName) {
    int customer_id = sqlCon.insertRailCoCustomers(firstName, lastName);
  }

  public void insertItem(int item_id, String item_name, String item_manf, double price) {
    int item_id = sqlCon.insertRailCoItems(item_id, item_name, item_manf, price);
  }

  public void insertCustomerPhone(int customer_id, String phone_number, String type) {
    int phone_id = sqlCon.insertRailCoCustomerPhones(customer_id, phone_number, type);
  }

  public void insertStore(String street_address, int zipcode, String city, String state) {
    int store_id = sqlCon.insertRailCoStores(street_address, zipcode, city, state);
  }

  public void insertTransaction(int item_id, int customer_id, int store_id, Timestamp timestamp) {
    int transaction_id = sqlCon.insertRailCoTransactions(item_id, customer_id, store_id, timestamp);
  }

  public void createTables() {
    sqlCon.createDatabaseTables();
  }

  public void displayAllTables() {
    System.out.println("\nCustomers Table:\n");
    showCustomersTable();
    System.out.println("\nItems Table:\n");
    showItemsTable();
    System.out.println("\nTransactions Table:\n");
    showTransactionsTable();
    System.out.println("\nStores Table:\n");
    showStoresTable();
    System.out.println("\nCustomers' Phones Table:\n");
    showCustomerPhonesTable();
  }

  public void showCustomersTable() {
    sqlCon.showTable("railCoCustomers");
  }
  public void showItemsTable() {
    sqlCon.showTable("railCoItems");
  }
  public void showTransactionsTable() {
    sqlCon.showTable("railCoTransactions");
  }
  public void showStoresTable() {
    sqlCon.showTable("railCoStores");
  }
  public void showCustomerPhonesTable() {
    sqlCon.showTable("railCoCustomerPhones");
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
