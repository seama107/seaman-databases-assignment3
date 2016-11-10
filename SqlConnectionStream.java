import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;



public class SqlConnectionStream {
  protected String driverLocation;
  protected String connectionURL;
  protected String mySqlUsername;
  protected String mySqlPassword;
  protected boolean hasDriver;

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

  public void closeConnections(AutoCloseable[] connections) {
    //To be called after all SQL querys and updates.
    for (AutoCloseable con : connections ) {
      if(con != null) {
        try{
          con.close();
        }
        catch(Exception e) {
          System.out.println("Error Closing stream");
          e.printStackTrace();
        }
      }

    }

  }

  public int executeUpdate(String updateStatement) {
    Connection mySql = null;
    int affectedRows = 0;
    try {
      mySql = DriverManager.getConnection(connectionURL, mySqlUsername, mySqlPassword);
      PreparedStatement pst = mySql.prepareStatement(updateStatement);
      affectedRows = pst.executeUpdate();
      //System.out.println("Executed update.");
    }
    catch(SQLException se){
      se.printStackTrace();
    }
    catch(Exception e){
      e.printStackTrace();
    }
    closeConnections(new AutoCloseable[]{mySql});
    return affectedRows;
  }

  public boolean databaseHasTable(String tableName) {
    Connection mySql = null;
    ResultSet rs = null;
    boolean output = false;
    try {
      mySql = DriverManager.getConnection(connectionURL, mySqlUsername, mySqlPassword);
      PreparedStatement pst = mySql.prepareStatement( String.format("SHOW TABLES LIKE '%s';", tableName ));
      rs = pst.executeQuery();
      rs.last();
      int resultSetSize = rs.getRow();
      output = (resultSetSize != 0);
    }
    catch(SQLException se){
      se.printStackTrace();
    }
    catch(Exception e){
      e.printStackTrace();
    }
    closeConnections(new AutoCloseable[]{mySql, rs});
    return output;
  }

  public void showTable(String tableName) {
    Connection mySql = null;
    ResultSet rs = null;
    try {
      mySql = DriverManager.getConnection(connectionURL, mySqlUsername, mySqlPassword);
      PreparedStatement pst = mySql.prepareStatement( String.format("SELECT * FROM %s;", tableName ));
      rs = pst.executeQuery();
      simplePrintResultSet(rs);
    }
    catch(SQLException se){
      se.printStackTrace();
    }
    catch(Exception e){
      e.printStackTrace();
    }
    closeConnections(new AutoCloseable[]{mySql, rs});
  }

  public void printDatabaseSchema() {
    showTable("information_schema.COLUMNS;");
  }

  public void simplePrintResultSet(ResultSet rs) throws Exception {
    ResultSetMetaData metaData = rs.getMetaData();
    int numColumns = metaData.getColumnCount();
    while(rs.next()) {
      for (int i = 1; i <= numColumns; ++i) {
        if(i > 1 && i <= numColumns ) {
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
