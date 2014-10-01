package org.aksw.linkedspending.converter;

import org.aksw.linkedspending.Scheduler;
import org.aksw.linkedspending.tools.PropertiesLoader;
import org.junit.Test;
import java.io.*;
import java.util.Properties;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration Test for the converter class
 */
public class ConverterIT
{
	private static final Properties	PROPERTIES		= PropertiesLoader.getProperties("environmentVariables.properties");
	private int						number, numberNew, fileCount;
	private File					converterDir	= new File(PROPERTIES.getProperty("pathRdf"));
	private String					convertPath		= PROPERTIES.getProperty("pathRdf");
	private String					randomFile;

	/**
	 * This test needs files to be converted. Beware situation where all downloaded files are
	 * already converted.
	 */
	@Test public void converterTest()
	{
		number = fileNumber(converterDir);

		Scheduler.runManually();
		Scheduler.runConverter();

		try
		{
			Thread.sleep(30000);
		}
		catch (InterruptedException e)
		{
			fail("Interrupted exception: " + e.getMessage());
		}

		Scheduler.stopConverter();

		numberNew = fileNumber(converterDir);

		System.out.println(number + " " + numberNew);

		if (number >= numberNew)
		{
			fail("Number of files not increased(This test needs files to be converted. Beware situation where all downloaded files are already converted.)");
		}
	}

	@Test public void consistencyCheckRandom()
	{
		fileCount = fileNumber(converterDir);
		int rand = (int) (Math.random() * fileCount);
		randomFile = converterDir.list()[rand];
		String line = "";

		System.out.println(convertPath + randomFile);

		try
		{
			FileReader fr = new FileReader(convertPath + randomFile);
			BufferedReader br = new BufferedReader(fr);

			int count = 0;
			while (br.readLine() != null)
			{
				count++;
			}
			// System.out.println(count);

			rand = (int) (Math.random() * count);

			// System.out.println(rand);

			fr = new FileReader(convertPath + randomFile);
			br = new LineNumberReader(fr);

			for (int i = 0; i < rand; i++)
			{
				line = br.readLine();
			}

		}
		catch (Exception e)
		{
			fail("Exception: " + e.getMessage());
		}
		// System.out.println(line);

		char[] lineChar = line.toCharArray();
		boolean correct = false;

		if (lineChar[0] == '<')
		{
			for (int i = 1; i < lineChar.length; i++)
			{
				if (lineChar[i] == '>' && lineChar[i + 1] == ' ' && lineChar[i + 2] == '<')
				{
					for (int j = i + 3; j < lineChar.length; j++)
					{
						if (lineChar[j] == '>' && lineChar[lineChar.length - 1] == '.')
						{
							correct = true;
						}
					}
				}
			}
		}

		assertTrue("A random line does not fit the N-triple convention", correct);

	}

	private int fileNumber(File directory)
	{
		int number = 0;
		for (File file : directory.listFiles())
		{
			if (file.isFile())
			{
				number++;
			}
		}
		return number;
	}
}
