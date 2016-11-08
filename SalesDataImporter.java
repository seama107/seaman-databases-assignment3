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
