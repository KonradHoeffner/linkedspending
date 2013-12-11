import static org.junit.Assert.assertEquals;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import org.junit.Test;

public class CodeToCurrencyTest
{
	@Test public void testCodeToCurrency() 
	{
		Map<String,String> codeToCurrency = new HashMap<>();
		try(Scanner in = new Scanner(this.getClass().getClassLoader().getResourceAsStream("codetocurrency.tsv")))
		{
			while(in.hasNextLine())
			{
				String line = in.nextLine();
				if(line.trim().isEmpty()) {continue;} 
				String[] tokens = line.split("\t");
				codeToCurrency.put(tokens[0], tokens[1]);
			}
		}
		assertEquals(codeToCurrency.get("JPY"),"http://dbpedia.org/resource/Japanese_yen");
	}
}