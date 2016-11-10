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
    sdi.importUnstructuredData();
    try {
      sdi.readTuple();
    }
    catch(Exception e) {
      e.printStackTrace();
    }
    sdi.displayAllTables();
    sdi.quitelyClose();
  }


}
