import java.io.InputStreamReader;
import java.io.BufferedReader;

public class DataImportClient {
  private SalesDataImporter sdi;

  public DataImportClient() {
    sdi = new SalesDataImporter();
  }

  public void runApplication() {
    if(!(sdi.hasSQLDriver()))
    {
      return;
    }
    //sdi.createTables();
    sdi.printDatabaseSchema();
    try {
      System.out.println(sdi.readTuple());
    }
    catch(Exception e) {
      e.printStackTrace();
    }
    sdi.quitelyClose();
  }


}
