import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import lombok.extern.java.Log;

@Log
public class Test
{
	public static void main(String[] args) 
	{
		Map<String,String> codeToCurrency = new HashMap<>();
		try(Scanner in = new Scanner(Test.class.getClassLoader().getResourceAsStream("codetocurrency.tsv")))
		{
			while(in.hasNextLine())
			{
				String line = in.nextLine();
				if(line.trim().isEmpty()) {continue;} 
				String[] tokens = line.split("\t");
				codeToCurrency.put(tokens[0], tokens[1]);
			}
		}
		System.out.println(codeToCurrency);
	}
}