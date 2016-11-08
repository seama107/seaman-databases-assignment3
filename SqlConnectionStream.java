import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;



public class SqlConnectionStream {
  private String driverLocation;
  private String connectionURL;
  private String mySqlUsername;
  private String mySqlPassword;
  private boolean hasDriver;

  public SqlConnectionStream(String driverLoc, String conURL, String username, String pass) {
    driverLocation = driverLoc;
    connectionURL = conURL;
    mySqlUsername = username;
    mySqlPassword = pass;
    hasDriver = false;
    try{
      Class.forName(driverLocation);
      hasDriver = true;
    }
    catch(Exception e){
      System.out.println("Driver could not be located.");
      System.out.println("Make sure the CLASSPATH environment variable holds the driver jar.");
      System.out.println("Example: 'export CLASSPATH=/Users/michaelseaman/Downloads/testConnector/mysql-connector-java-5.1.39:$CLASSPATH'");
    }

  }

  public SqlConnectionStream() {
    this("com.mysql.jdbc.Driver", "jdbc:mysql://us-cdbr-azure-west-b.cleardb.com:3306/acsm_676dbbe84ea3d8c", "be234364e375d8", "e60754d2");
  }

  public int executeUpdate() {
    return 1;
  }


  public void closeConnections(Connection mySql, AutoCloseable auxilary) {
    //To be called after all SQL querys and updates.
    //Closes the primary connection object and whatever secondary
    //Stream "auxilary" quietly
    try{
      mySql.close();
      auxilary.close();
    }
    catch(Exception e) {
      e.printStackTrace();
    }
    finally {
      try {
        mySql.close();
      }
      catch(Exception e) {}

      try {
        auxilary.close();
      }
      catch(Exception e) {}
    }

  }

  private int executeUpdate(String updateStatement) {
    Connection mySql = null;
    Statement statement = null;
    int affectedRows = 0;
    try {
      mySql = DriverManager.getConnection(connectionURL, mySqlUsername, mySqlPassword);
      statement = mySql.createStatement();
      affectedRows = statement.executeUpdate(updateStatement);
      System.out.println("Executed update.");
    }
    catch(SQLException se){
      se.printStackTrace();
    }
    catch(Exception e){
      e.printStackTrace();
    }
    closeConnections(mySql, statement);
    return affectedRows;
  }

  public void createTable(String tableCreateStatement) {
    executeUpdate(tableCreateStatement);
  }

  public void printDatabaseSchema() {
    Connection mySql = null;
    ResultSet rs = null;
    try {
      mySql = DriverManager.getConnection(connectionURL, mySqlUsername, mySqlPassword);
      PreparedStatement pst = mySql.prepareStatement("SELECT * FROM information_schema.COLUMNS;");
      rs = pst.executeQuery();
      simplePrintResultSet(rs);
    }
    catch(Exception e){
      e.printStackTrace();
    }
    closeConnections(mySql, rs);

  }

  public void simplePrintResultSet(ResultSet rs) throws Exception {
    ResultSetMetaData metaData = rs.getMetaData();
    int numColumns = metaData.getColumnCount();
    while(rs.next()) {
      for (int i = 1; i <= numColumns; ++i) {
        if(i > 1 && i != numColumns) {
          System.out.print(",  ");
        }
        System.out.print(rs.getString(i));

      }
      System.out.println("");
    }
  }

  public boolean hasDriver() {
    return hasDriver;
  }


}
