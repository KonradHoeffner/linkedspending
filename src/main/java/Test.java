import java.util.logging.Level;
import java.util.logging.LogManager;
import lombok.extern.java.Log;

@Log
public class Test
{
	public static void main(String[] args)
	{
		System.setProperty( "java.util.logging.config.file", "src/main/resources/logging.properties" );
		try { LogManager.getLogManager().readConfiguration(); } catch ( Exception e ) { e.printStackTrace();}
		log.setLevel(Level.FINE);
		System.out.println("Logging level is: " + log.getLevel());
		log.info("info");  
		log.fine("fine");
	}

}
