import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;

public class RailCoConnectionStream extends SqlConnectionStream {

    public RailCoConnectionStream() {
      super("com.mysql.jdbc.Driver", "jdbc:mysql://us-cdbr-azure-west-b.cleardb.com:3306/acsm_676dbbe84ea3d8c", "be234364e375d8", "e60754d2");
    }

    public int insertRailCoCustomers(String firstName, String lastName) {
      Connection mySql = null;
      ResultSet rs = null;
      int customer_id = -1;
      try {
        mySql = DriverManager.getConnection(connectionURL, mySqlUsername, mySqlPassword);
        String insertString = "INSERT INTO railCoCustomers(first_name, last_name) VALUES (?, ?);";
        PreparedStatement pst = mySql.prepareStatement(insertString);
        pst.setString(1, firstName);
        pst.setString(2, lastName);
        System.out.println(pst);
        int affectedRows = pst.executeUpdate();
        printInsertFeedback(affectedRows);
        if(affectedRows > 0) {
          customer_id = lastInsertID(mySql, rs);
        }

      }
      catch(SQLException se){
        se.printStackTrace();
      }
      catch(Exception e){
        e.printStackTrace();
      }
      closeConnections(new AutoCloseable[]{mySql, rs});
      return customer_id;
    }

    public int insertRailCoItems(int item_id, String item_name, String item_manf, double price) {
      Connection mySql = null;
      ResultSet rs = null;
      int item_return_id = -1;
      try {
        mySql = DriverManager.getConnection(connectionURL, mySqlUsername, mySqlPassword);
        String insertString = "INSERT INTO railCoItems(item_id, item_name, item_manf, price) VALUES (?, ?, ?, ?);";
        PreparedStatement pst = mySql.prepareStatement(insertString);
        pst.setInt(1, item_id);
        pst.setString(2, item_name);
        pst.setString(3, item_manf);
        pst.setDouble(4, price);
        System.out.println(pst);
        int affectedRows = pst.executeUpdate();
        printInsertFeedback(affectedRows);
        if(affectedRows > 0) {
          item_return_id = lastInsertID(mySql, rs);
        }
      }
      catch(SQLException se){
        se.printStackTrace();
      }
      catch(Exception e){
        e.printStackTrace();
      }
      closeConnections(new AutoCloseable[]{mySql});
      return item_return_id;
    }

    public int insertRailCoCustomerPhones(int customer_id, String phone_number, String type) {
      Connection mySql = null;
      ResultSet rs = null;
      int phone_id = -1;
      try {
        mySql = DriverManager.getConnection(connectionURL, mySqlUsername, mySqlPassword);
        String insertString = "INSERT INTO railCoCustomerPhones(customer_id, phone_number, type) VALUES (?, ?, ?);";
        PreparedStatement pst = mySql.prepareStatement(insertString);
        pst.setInt(1, customer_id);
        pst.setString(2, phone_number);
        pst.setString(3, type);
        System.out.println(pst);
        int affectedRows = pst.executeUpdate();
        printInsertFeedback(affectedRows);
        if(affectedRows > 0) {
          phone_id = lastInsertID(mySql, rs);
        }
      }
      catch(SQLException se){
        se.printStackTrace();
      }
      catch(Exception e){
        e.printStackTrace();
      }
      closeConnections(new AutoCloseable[]{mySql});
      return phone_id;
    }

    public int insertRailCoStores(String street_address, int zipcode, String city, String state) {
      Connection mySql = null;
      ResultSet rs = null;
      int store_id = -1;
      try {
        mySql = DriverManager.getConnection(connectionURL, mySqlUsername, mySqlPassword);
        String insertString = "INSERT INTO railCoStores(street_address, zipcode, city, state) VALUES (?, ?, ?, ?);";
        PreparedStatement pst = mySql.prepareStatement(insertString);
        pst.setString(1, street_address);
        pst.setInt(2, zipcode);
        pst.setString(3, city);
        pst.setString(4, state);
        System.out.println(pst);
        int affectedRows = pst.executeUpdate();
        printInsertFeedback(affectedRows);
        if(affectedRows > 0) {
          store_id = lastInsertID(mySql, rs);
        }
      }
      catch(SQLException se){
        se.printStackTrace();
      }
      catch(Exception e){
        e.printStackTrace();
      }
      closeConnections(new AutoCloseable[]{mySql});
      return store_id;
    }

    public int insertRailCoTransactions(int item_id, int customer_id, int store_id, Timestamp timestamp) {
      Connection mySql = null;
      ResultSet rs = null;
      int transaction_id = -1;
      try {
        mySql = DriverManager.getConnection(connectionURL, mySqlUsername, mySqlPassword);
        String insertString = "INSERT INTO railCoTransactions(item_id, customer_id, store_id, timestamp) VALUES (?, ?, ?, ?);";
        PreparedStatement pst = mySql.prepareStatement(insertString);
        pst.setInt(1, item_id);
        pst.setInt(2, customer_id);
        pst.setInt(3, store_id);
        pst.setTimestamp(4, timestamp);
        System.out.println(pst);
        int affectedRows = pst.executeUpdate();
        printInsertFeedback(affectedRows);
        if(affectedRows > 0) {
          transaction_id = lastInsertID(mySql, rs);
        }
      }
      catch(SQLException se){
        se.printStackTrace();
      }
      catch(Exception e){
        e.printStackTrace();
      }
      closeConnections(new AutoCloseable[]{mySql});
      return transaction_id;
    }

    public void printInsertFeedback(int rowsAffected) {
      if(rowsAffected == 0) {
        System.out.println("Unsucessful Insert.");
      }
      else {
        System.out.println( String.format( "Inserted successfully, %d rows affected.", rowsAffected));
      }
    }

    public int lastInsertID(Connection mySql, ResultSet rs) throws Exception {
      PreparedStatement pst = mySql.prepareStatement("SELECT LAST_INSERT_ID();");
      rs = pst.executeQuery();
      rs.next();
      String s =  rs.getString(1);
      System.out.println(s);
      int output = Integer.parseInt(s);
      System.out.println(output);
      return output;
    }

    public void createDatabaseTables() {
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


      executeUpdate(createCustomersStatement);
      executeUpdate(createCustomerPhonesStatement);
      executeUpdate(createStoresStatement);
      executeUpdate(createItemsStatement);
      executeUpdate(createTransactionsStatement);
    }
}
