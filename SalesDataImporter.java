import java.io.FileReader;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;


public class SalesDataImporter {

  private RailCoConnectionStream sqlCon;

  public SalesDataImporter() {
    sqlCon = new RailCoConnectionStream();
  }

  public void importUnstructuredData(String filePath) {
    //Opening file
    File dataCSVFile = new File(filePath);
    BufferedReader dataReader;
    try {
      dataReader = new BufferedReader(new FileReader(dataCSVFile));
    }
    catch(IOException e) {
      System.out.println("Error opening file:");
      e.printStackTrace();
      return;
    }

    //Reading file
    String line = null;
    int lineNumber = 1;
    try {
      while((line = dataReader.readLine()) != null){
        System.out.println(String.format("Processing Line %d", lineNumber++));
        processTuple(line.replace("\"","").split(","));
      }
    }
    catch (Exception e) {
      System.out.println("Error occured whilst reading file.");
      e.printStackTrace();
    }

    //Closing file
    try {
      dataReader.close();
    }
    catch(Exception e) {
      System.out.println("Error closing data file:");
      e.printStackTrace();
    }

  }


  public void processTuple(String[] tuple){
    /*
    Format for incoming tuples:
    timestamp,address,zip,city,state,item_id,item_name,item_manf,price,first,last,home,cell

    This method first checks the 5 tables to see iff each relation is already present in them
    and when the relation is new, it insers them
    */
    int zip;
    double price;
    int item_id;
    try {
      zip = Integer.parseInt(tuple[2]);
      price = Double.parseDouble(tuple[8]);
      item_id = Integer.parseInt(tuple[5]);
    }
    catch(Exception e) {
      System.out.println("Error reading values on file.");
      e.printStackTrace();
      return;
    }
    InsertResponse customerMembership = insertCustomer(tuple[9], tuple[10]);
    InsertResponse storeMembership = insertStore(tuple[1], zip, tuple[3], tuple[4]);
    InsertResponse itemMembership = insertItem(item_id, tuple[6], tuple[7], price);
    if(customerMembership.newEntry) {
      System.out.print("\tInserting new phone numbers for new customer: ");
      if( !(tuple[11].equals("null"))) {
        insertCustomerPhone(customerMembership.id, tuple[11], "home");
        System.out.print("home ");
      }
      if( !(tuple[12].equals("null"))) {
        insertCustomerPhone(customerMembership.id, tuple[12], "cell");
        System.out.print("cell ");
      }
      System.out.println("");
    }
    if(customerMembership.newEntry || storeMembership.newEntry || itemMembership.newEntry) {
      insertTransaction(itemMembership.id, customerMembership.id, storeMembership.id, Timestamp.valueOf(tuple[0]) );
    }
  }

  public InsertResponse insertCustomer(String firstName, String lastName) {
    InsertResponse membership = sqlCon.getCustomerMembership(firstName, lastName);
    if(membership.newEntry) {
        System.out.println( String.format("\tNew customer: %s %s", firstName, lastName));
        membership.id = sqlCon.insertRailCoCustomers(firstName, lastName);
    }
    else {
      System.out.println(String.format( "\tExisting customer with ID %d", membership.id));
    }
    return membership;
  }

  public InsertResponse insertItem(int item_id, String item_name, String item_manf, double price) {
    InsertResponse membership = sqlCon.getItemMembership(item_id);
    if(membership.newEntry) {
        System.out.println( String.format("\tNew item [ # %d]: %s by %s costing %f", item_id, item_name, item_manf, price));
        membership.id = sqlCon.insertRailCoItems(item_id, item_name, item_manf, price);
    }
    else {
      System.out.println(String.format( "\tExisting item with ID %d", membership.id));
    }
    return membership;
  }

  public InsertResponse insertCustomerPhone(int customer_id, String phone_number, String type) {
    InsertResponse membership = sqlCon.getPhoneMembership(customer_id, phone_number, type);
    if(membership.newEntry) {
        membership.id = sqlCon.insertRailCoCustomerPhones(customer_id, phone_number, type);
    }
    else {
      System.out.println(String.format("\tExisting phone with ID %d", membership.id));
    }
    return membership;
  }

  public InsertResponse insertStore(String street_address, int zipcode, String city, String state) {
    InsertResponse membership = sqlCon.getStoreMembership(street_address, zipcode, city, state);
    if(membership.newEntry) {
        System.out.println( String.format("\tNew store on %s in %d %s, %s", street_address, zipcode, city, state));
        membership.id = sqlCon.insertRailCoStores(street_address, zipcode, city, state);
    }
    else {
      System.out.println(String.format( "\tExisting store with ID %d", membership.id));
    }
    return membership;
  }

  public InsertResponse insertTransaction(int item_id, int customer_id, int store_id, Timestamp timestamp) {
    InsertResponse membership = sqlCon.getTransactionMembership(item_id, customer_id, store_id, timestamp);
    if(membership.newEntry) {
        membership.id = sqlCon.insertRailCoTransactions(item_id, customer_id, store_id, timestamp);
    }
    else {
      System.out.println(String.format( "\tExisting tranaction with ID %d", membership.id));
    }
    return membership;
  }

  public boolean checkIfExistingTables() {
    boolean output = (sqlCon.databaseHasTable("railCoCustomerPhones")) ||
    (sqlCon.databaseHasTable("railCoTransactions")) ||
    (sqlCon.databaseHasTable("railCoCustomers")) ||
    (sqlCon.databaseHasTable("railCoStores")) ||
    (sqlCon.databaseHasTable("railCoItems"));
    return output;
  }

  public void resetTables() {
    sqlCon.resetDatabaseTables();
  }

  public void createTables() {
    sqlCon.createDatabaseTables();
  }

  public void dropTables() {
    sqlCon.dropDatabaseTables();
  }

  public void displayAllTables() {
    showCustomersTable();
    showItemsTable();
    showTransactionsTable();
    showStoresTable();
    showCustomerPhonesTable();
  }

  public void showCustomersTable() {
    System.out.println("\nCustomers Table:\n");
    System.out.println("ID, first, last");
    sqlCon.showTable("railCoCustomers");
  }
  public void showItemsTable() {
    System.out.println("\nItems Table:\n");
    System.out.println("ID, name, manf, price");
    sqlCon.showTable("railCoItems");
  }
  public void showTransactionsTable() {
    System.out.println("\nTransactions Table:\n");
    System.out.println("ID, item_id, customer_id, store_id, timestamp");
    sqlCon.showTable("railCoTransactions");
  }
  public void showStoresTable() {
    System.out.println("\nStores Table:\n");
    System.out.println("ID, address, zip, city, state");
    sqlCon.showTable("railCoStores");
  }
  public void showCustomerPhonesTable() {
    System.out.println("\nCustomers' Phones Table:\n");
    System.out.println("ID, phone number, customer_id, type");
    sqlCon.showTable("railCoCustomerPhones");
  }

  public void printDatabaseSchema() {
    sqlCon.printDatabaseSchema();
  }

  public boolean hasSQLDriver() {
    return sqlCon.hasDriver();
  }

}
