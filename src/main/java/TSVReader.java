import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

public class TSVReader implements Closeable 
{
	final Scanner in;
	String peekLine = null;

	public TSVReader(File f) throws FileNotFoundException
	{
		System.out.println(f);
		in = new Scanner(f);
	}
	
	boolean hasNextTokens()
	{
		if(peekLine!=null) return true;
		if(!in.hasNextLine()) {return false;}
		String line = in.nextLine().trim();
		if(line.isEmpty())  {return hasNextTokens();}
		this.peekLine = line;		
		return true;		
	}

	String[] nextTokens()
	{
		if(!hasNextTokens()) return null;		
		String[] tokens = peekLine.split("[\\s\t]+");
//		System.out.println(Arrays.toString(tokens));
		peekLine=null;		
		return tokens;
	}
	
	@Override public void close() throws IOException {in.close();}
}