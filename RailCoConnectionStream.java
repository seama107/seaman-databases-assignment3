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
      PreparedStatement pst = null;
      int customer_id = -1;
      try {
        mySql = DriverManager.getConnection(connectionURL, mySqlUsername, mySqlPassword);
        String insertString = "INSERT INTO railCoCustomers(first_name, last_name) VALUES (?, ?);";
        pst = mySql.prepareStatement(insertString);
        pst = prepareCustomerStatement(pst, firstName, lastName);
        int affectedRows = pst.executeUpdate();
        printInsertFeedback(affectedRows);
        if(affectedRows > 0) {
          customer_id = lastInsertID(mySql, rs);
        }

      }
      catch(SQLException se){
        System.out.println("PreparedStatement:");
        System.out.println(pst);
        se.printStackTrace();
      }
      catch(Exception e){
        e.printStackTrace();
      }
      closeConnections(new AutoCloseable[]{mySql, rs});
      return customer_id;
    }

    public PreparedStatement prepareCustomerStatement(PreparedStatement pst, String firstName, String lastName) throws SQLException {
      pst.setString(1, firstName);
      pst.setString(2, lastName);
      return pst;
    }

    public InsertResponse getCustomerMembership(String firstName, String lastName) {
      Connection mySql = null;
      ResultSet rs = null;
      PreparedStatement pst = null;
      InsertResponse tableMembership = new InsertResponse();
      sqlBlock: try {
        mySql = DriverManager.getConnection(connectionURL, mySqlUsername, mySqlPassword);
        String insertString = "SELECT * FROM railCoCustomers WHERE (first_name, last_name) = (?, ?);";
        pst = mySql.prepareStatement(insertString);
        pst = prepareCustomerStatement(pst, firstName, lastName);
        rs = pst.executeQuery();
        tableMembership = getTableMembership(rs);
      }
      catch(SQLException se){
        System.out.println("PreparedStatement:");
        System.out.println(pst);
        se.printStackTrace();
      }
      catch(Exception e){
        e.printStackTrace();
      }
      closeConnections(new AutoCloseable[]{mySql, rs});
      return tableMembership;
    }

    public int insertRailCoItems(int item_id, String item_name, String item_manf, double price) {
      Connection mySql = null;
      ResultSet rs = null;
      PreparedStatement pst = null;
      int item_return_id = -1;
      try {
        mySql = DriverManager.getConnection(connectionURL, mySqlUsername, mySqlPassword);
        String insertString = "INSERT INTO railCoItems(item_id, item_name, item_manf, price) VALUES (?, ?, ?, ?);";
        pst = mySql.prepareStatement(insertString);
        pst = prepareItemStatement(pst, item_id, item_name, item_manf, price);
        int affectedRows = pst.executeUpdate();
        printInsertFeedback(affectedRows);
        if(affectedRows > 0) {
          item_return_id = item_id;
        }
      }
      catch(SQLException se){
        System.out.println("PreparedStatement:");
        System.out.println(pst);
        se.printStackTrace();
      }
      catch(Exception e){
        e.printStackTrace();
      }
      closeConnections(new AutoCloseable[]{mySql});
      return item_return_id;
    }

    public PreparedStatement prepareItemStatement(PreparedStatement pst, int item_id, String item_name, String item_manf, double price) throws SQLException {
      pst.setInt(1, item_id);
      pst.setString(2, item_name);
      pst.setString(3, item_manf);
      pst.setDouble(4, price);
      return pst;
    }

    public InsertResponse getItemMembership(int item_id) {
      Connection mySql = null;
      ResultSet rs = null;
      PreparedStatement pst = null;
      InsertResponse tableMembership = new InsertResponse();
      sqlBlock: try {
        mySql = DriverManager.getConnection(connectionURL, mySqlUsername, mySqlPassword);
        String insertString = "SELECT * FROM railCoItems WHERE (item_id) = (?);";
        pst = mySql.prepareStatement(insertString);
        pst.setInt(1,item_id);
        rs = pst.executeQuery();
        tableMembership = getTableMembership(rs);
      }
      catch(SQLException se){
        System.out.println("PreparedStatement");
        System.out.println(pst);
        se.printStackTrace();
      }
      catch(Exception e){
        e.printStackTrace();
      }
      closeConnections(new AutoCloseable[]{mySql, rs});
      return tableMembership;
    }

    public int insertRailCoCustomerPhones(int customer_id, String phone_number, String type) {
      Connection mySql = null;
      ResultSet rs = null;
      PreparedStatement pst = null;
      int phone_id = -1;
      try {
        mySql = DriverManager.getConnection(connectionURL, mySqlUsername, mySqlPassword);
        String insertString = "INSERT INTO railCoCustomerPhones(customer_id, phone_number, type) VALUES (?, ?, ?);";
        pst = mySql.prepareStatement(insertString);
        pst = preparePhoneStatement(pst, customer_id, phone_number, type);
        int affectedRows = pst.executeUpdate();
        if(affectedRows > 0) {
          phone_id = lastInsertID(mySql, rs);
        }
      }
      catch(SQLException se){
        System.out.println("PreparedStatement:");
        System.out.println(pst);
        se.printStackTrace();
      }
      catch(Exception e){
        e.printStackTrace();
      }
      closeConnections(new AutoCloseable[]{mySql});
      return phone_id;
    }

    public PreparedStatement preparePhoneStatement(PreparedStatement pst, int customer_id, String phone_number, String type)  throws SQLException {
      pst.setInt(1, customer_id);
      pst.setString(2, phone_number);
      pst.setString(3, type);
      return pst;
    }

    public InsertResponse getPhoneMembership(int customer_id, String phone_number, String type) {
      //Returns -1 if phone# is not in the table, and the id
      //of the phone# if they are in the table
      Connection mySql = null;
      ResultSet rs = null;
      PreparedStatement pst = null;
      InsertResponse tableMembership = new InsertResponse();
      sqlBlock: try {
        mySql = DriverManager.getConnection(connectionURL, mySqlUsername, mySqlPassword);
        String insertString = "SELECT * FROM railCoCustomerPhones WHERE (customer_id, phone_number, type) = (?, ?, ?);";
        pst = mySql.prepareStatement(insertString);
        pst = preparePhoneStatement(pst, customer_id, phone_number, type);
        rs = pst.executeQuery();
        tableMembership = getTableMembership(rs);
      }
      catch(SQLException se){
        System.out.println("PreparedStatement:");
        System.out.println(pst);
        se.printStackTrace();
      }
      catch(Exception e){
        e.printStackTrace();
      }
      closeConnections(new AutoCloseable[]{mySql, rs});
      return tableMembership;
    }

    public int insertRailCoStores(String street_address, int zipcode, String city, String state) {
      Connection mySql = null;
      ResultSet rs = null;
      PreparedStatement pst = null;
      int store_id = -1;
      try {
        mySql = DriverManager.getConnection(connectionURL, mySqlUsername, mySqlPassword);
        String insertString = "INSERT INTO railCoStores(street_address, zipcode, city, state) VALUES (?, ?, ?, ?);";
        pst = mySql.prepareStatement(insertString);
        pst = prepareStoresStatement(pst, street_address, zipcode, city, state);
        int affectedRows = pst.executeUpdate();
        printInsertFeedback(affectedRows);
        if(affectedRows > 0) {
          store_id = lastInsertID(mySql, rs);
        }
      }
      catch(SQLException se){
        System.out.println("PreparedStatement:");
        System.out.println(pst);
        se.printStackTrace();
      }
      catch(Exception e){
        e.printStackTrace();
      }
      closeConnections(new AutoCloseable[]{mySql});
      return store_id;
    }

    public PreparedStatement prepareStoresStatement(PreparedStatement pst, String street_address, int zipcode, String city, String state) throws SQLException  {
      pst.setString(1, street_address);
      pst.setInt(2, zipcode);
      pst.setString(3, city);
      pst.setString(4, state);
      return pst;
    }

    public InsertResponse getStoreMembership(String street_address, int zipcode, String city, String state) {
      Connection mySql = null;
      ResultSet rs = null;
      PreparedStatement pst = null;
      InsertResponse tableMembership = new InsertResponse();
      sqlBlock: try {
        mySql = DriverManager.getConnection(connectionURL, mySqlUsername, mySqlPassword);
        String insertString = "SELECT * FROM railCoStores WHERE (street_address, zipcode, city, state) = (?, ?, ?, ?);";
        pst = mySql.prepareStatement(insertString);
        pst = prepareStoresStatement(pst, street_address, zipcode, city, state);
        rs = pst.executeQuery();
        tableMembership = getTableMembership(rs);
      }
      catch(SQLException se){
        System.out.println("PreparedStatement:");
        System.out.println(pst);
        se.printStackTrace();
      }
      catch(Exception e){
        e.printStackTrace();
      }
      closeConnections(new AutoCloseable[]{mySql, rs});
      return tableMembership;
    }

    public int insertRailCoTransactions(int item_id, int customer_id, int store_id, Timestamp timestamp) {
      Connection mySql = null;
      ResultSet rs = null;
      PreparedStatement pst = null;
      int transaction_id = -1;
      try {
        mySql = DriverManager.getConnection(connectionURL, mySqlUsername, mySqlPassword);
        String insertString = "INSERT INTO railCoTransactions(item_id, customer_id, store_id, timestamp) VALUES (?, ?, ?, ?);";
        pst = mySql.prepareStatement(insertString);
        pst = prepareTransactionStatement(pst, item_id, customer_id, store_id, timestamp);
        int affectedRows = pst.executeUpdate();
        printInsertFeedback(affectedRows);
        if(affectedRows > 0) {
          transaction_id = lastInsertID(mySql, rs);
        }
      }
      catch(SQLException se){
        System.out.println("PreparedStatement:");
        System.out.println(pst);
        se.printStackTrace();
      }
      catch(Exception e){
        e.printStackTrace();
      }
      closeConnections(new AutoCloseable[]{mySql});
      return transaction_id;
    }

    public PreparedStatement prepareTransactionStatement(PreparedStatement pst, int item_id, int customer_id, int store_id, Timestamp timestamp) throws SQLException  {
      pst.setInt(1, item_id);
      pst.setInt(2, customer_id);
      pst.setInt(3, store_id);
      pst.setTimestamp(4, timestamp);
      return pst;
    }


    public InsertResponse getTransactionMembership(int item_id, int customer_id, int store_id, Timestamp timestamp) {
      Connection mySql = null;
      ResultSet rs = null;
      PreparedStatement pst = null;
      InsertResponse tableMembership = new InsertResponse();
      sqlBlock: try {
        mySql = DriverManager.getConnection(connectionURL, mySqlUsername, mySqlPassword);
        String insertString = "SELECT * FROM railCoTransactions WHERE (item_id, customer_id, store_id, timestamp) = (?, ?, ?, ?);";
        pst = mySql.prepareStatement(insertString);
        pst = prepareTransactionStatement(pst, item_id, customer_id, store_id, timestamp);
        rs = pst.executeQuery();
        tableMembership = getTableMembership(rs);
      }
      catch(SQLException se){
        System.out.println("PreparedStatement:");
        System.out.println(pst);
        se.printStackTrace();
      }
      catch(Exception e){
        e.printStackTrace();
      }
      closeConnections(new AutoCloseable[]{mySql, rs});
      return tableMembership;
    }

    public InsertResponse getTableMembership(ResultSet rs) throws Exception {
      int id = -1;
      boolean newMembership = true;
      rs.last();
      int resultSetSize = rs.getRow();
      if(resultSetSize != 0) {
        //Meaning there is an existing record already in the table
        rs.first();
        String firstIDString =  rs.getString(1);
        rs.beforeFirst();
        id = Integer.parseInt(firstIDString);
        newMembership = false;
      }
      return new InsertResponse(newMembership, id);
    }


    public void printInsertFeedback(int rowsAffected) {
      if(rowsAffected == 0) {
        System.out.println("Unsucessful Insert.");
      }
      else {
        System.out.println( String.format( "\t\tInserted successfully, %d rows affected.", rowsAffected));
      }
    }

    public int lastInsertID(Connection mySql, ResultSet rs) throws Exception {
      PreparedStatement pst = mySql.prepareStatement("SELECT LAST_INSERT_ID();");
      rs = pst.executeQuery();
      return returnIntFromSingletonResult(rs);
    }

    public int returnIntFromSingletonResult(ResultSet rs) throws Exception {
      rs.first();
      String singleValue =  rs.getString(1);
      int output = Integer.parseInt(singleValue);
      return output;
    }

    public void resetDatabaseTables() {
      dropDatabaseTables();
      createDatabaseTables();
    }


    public void dropDatabaseTables() {
      //Not to be used in the application, just for cleaning and debugging

      String dropTransactionsStatement = "DROP TABLE railCoTransactions;";
      String dropCustomerPhonesStatement = "DROP TABLE railCoCustomerPhones;";
      String dropCustomersStatement = "DROP TABLE railCoCustomers;";
      String dropStoresStatement = "DROP TABLE railCoStores;";
      String dropItemsStatement = "DROP TABLE railCoItems;";

      if(databaseHasTable("railCoCustomerPhones")) {
        System.out.println("Deleteing railCoCustomerPhones");
        executeUpdate(dropCustomerPhonesStatement);
      }
      if(databaseHasTable("railCoTransactions")) {
        System.out.println("Deleteing railCoTransactions");
        executeUpdate(dropTransactionsStatement);
      }
      if(databaseHasTable("railCoCustomers")) {
        System.out.println("Deleteing railCoCustomers");
        executeUpdate(dropCustomersStatement);
      }
      if(databaseHasTable("railCoStores")) {
        System.out.println("Deleteing railCoStores");
        executeUpdate(dropStoresStatement);
      }
      if(databaseHasTable("railCoItems")) {
        System.out.println("Deleteing railCoItems");
        executeUpdate(dropItemsStatement);
      }
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

    if(!(databaseHasTable("railCoCustomers"))) {
      System.out.println("Creating railCoCustomers");
      executeUpdate(createCustomersStatement);
    }
    if(!(databaseHasTable("railCoStores"))) {
      System.out.println("Creating railCoStores");
      executeUpdate(createStoresStatement);
    }
    if(!(databaseHasTable("railCoItems"))) {
      System.out.println("Creating railCoStores");
      executeUpdate(createItemsStatement);
    }
    if(!(databaseHasTable("railCoCustomerPhones"))) {
      System.out.println("Creating railCoCustomerPhones");
      executeUpdate(createCustomerPhonesStatement);
    }
    if(!(databaseHasTable("railCoTransactions"))) {
      System.out.println("Creating railCoTransactions");
      executeUpdate(createTransactionsStatement);
    }
  }
}
