import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.IOException;

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

    BufferedReader br = new BufferedReader( new InputStreamReader(System.in));
    System.out.println("Welcome to the Rail Co. (TM) Data import application.");

    try {

      if(sdi.checkIfExistingTables()) {
        System.out.println("Existing Rail Co. relations seem to already exist on this server.");
        System.out.print("Would you like to reset them for this demo (Y/N)? ");
        String userInput = br.readLine();
        if(userInput.toUpperCase().charAt(0) == 'N') {
          System.out.println("Existing tables will be used.");
        }
        else {
          System.out.println("Resetting tables....");
          sdi.resetTables();
          System.out.println("Done.");
        }

      }


      System.out.print("Enter the name of your csv file: ");
      sdi.importUnstructuredData( br.readLine());
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    System.out.println("");
    System.out.println("Done");
    System.out.println("");
    System.out.println("");
    System.out.println("Completed Tables:");
    sdi.displayAllTables();
  }


}
