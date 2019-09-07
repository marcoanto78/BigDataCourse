import java.io.*;
import java.net.Socket;

/**
 * Created by Sparta on 5/20/2017.
 */
public class Client {
    public static void main(String[] args) throws IOException {
        Socket sc = null; // Need to initialize as we are closing in finally block
        BufferedReader bf = null;
        DataOutputStream dout = null;
        try{
			while(true){
				//Binding socket to 50001 port number on localhost
				sc = new Socket("localhost",9999);
				/* DataOutputStream: Writes any primitive type to an output stream
				It accepts any OutputStream object as constructor parameter to write on.
				getOutputStream method of Socket class will return OutputStream object
				attached to that socket instance
				*/
				dout = new DataOutputStream(sc.getOutputStream());
				dout.writeUTF("Hello Server" + "\n");
			}
        }
        catch (IOException e){
            e.printStackTrace();
        }
        finally {
            /* Close all IO streams
            Following both statements may throw an IOException, thats why throws statement
            of main method has been included
            */
            bf.close();
            sc.close();
            dout.close();
        }
    }
}