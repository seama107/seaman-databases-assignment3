public class InsertResponse{
  public boolean newEntry;
  public int id;

  public InsertResponse(boolean n, int i) {
    newEntry = n;
    id = i;
  }

  public InsertResponse() {
    newEntry = true;
    id = -1;
  }
}
