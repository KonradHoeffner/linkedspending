package org.aksw.linkedspending.old;

import com.hp.hpl.jena.rdf.model.Model;
import lombok.extern.java.Log;
import org.aksw.linkedspending.old.ConverterOld;
import org.aksw.linkedspending.tools.DataModel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.*;
import java.util.Set;
import java.util.TreeSet;
import static org.junit.Assert.fail;

@Log public class ConverterTest
{

	private final String	DATASET	= "2013";

	@Before public void copyFiles()
	{
		File testFile = new File("json/" + DATASET);
		if (testFile.exists())
		{
			testFile.renameTo(new File("json/" + DATASET + "_tmp"));
		}
		InputStream inStream = null;
		FileOutputStream outStream = null;
		try
		{
			inStream = ConverterTest.class.getClassLoader().getResourceAsStream(DATASET);
			outStream = new FileOutputStream(testFile);
			int read;
			byte[] bytes = new byte[1024];

			while ((read = inStream.read(bytes)) != -1)
			{
				outStream.write(bytes, 0, read);
			}
		}
		catch (IOException e)
		{
			fail("Could not copy test file");
		}
		finally
		{
			if (inStream != null)
			{
				try
				{
					inStream.close();
				}
				catch (IOException e)
				{
					log.warning("Error closing input stream: " + e.getMessage());
				}
			}
			if (outStream != null)
			{
				try
				{
					outStream.close();
				}
				catch (IOException e)
				{
					log.warning("Error closing output stream: " + e.getMessage());
				}
			}
		}
	}

	@After public void deleteFiles()
	{
		File test = new File("json/" + DATASET);
		if (test.exists())
		{
			test.delete();
		}
		File tmpFile = new File("json/" + DATASET + "_tmp");
		if (tmpFile.exists())
		{
			tmpFile.renameTo(test);
		}
	}

	@Test public void testCreateDataset()
	{
		Set<String> datasetSet = new TreeSet<>();

		BufferedReader datasetReader = new BufferedReader(new InputStreamReader(ConverterTest.class.getClassLoader()
				.getResourceAsStream(DATASET + ".nt")));

		try
		{
			String line;
			while ((line = datasetReader.readLine()) != null)
			{
				datasetSet.add(line.replaceAll("_:.*", ""));
			}
		}
		catch (IOException e)
		{
			fail("Could not load example converted data: " + e);
		}
		finally
		{
			try
			{
				if (datasetReader != null)
				{
					datasetReader.close();
				}
			}
			catch (IOException e)
			{
				log.warning("Error closing buffered reader: " + e.getMessage());
			}
		}

		ByteArrayOutputStream datasetOut = new ByteArrayOutputStream();

		Model model = DataModel.newModel();
		try
		{
			ConverterOld.createDataset(DATASET, model, datasetOut);
		}
		catch (Exception e)
		{
			fail("Exception: " + e.getMessage());
		}

		BufferedReader datasetIn = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(datasetOut.toByteArray())));
		try
		{
			String line;
			while ((line = datasetIn.readLine()) != null)
			{
				if (line.contains("uri-terms/created"))
				{
					continue;
				}
				if (datasetSet.contains(line.replaceAll("_:.*", "")))
				{
					datasetSet.remove(line.replaceAll("_:.*", ""));
				}
				else
				{
					fail("Got data which is not in example data: " + line);
				}
			}
		}
		catch (IOException e)
		{
			fail("Exception while parsing output: " + e);
		}

		for (String dataset : datasetSet)
		{
			if (dataset.contains("uri-terms/created"))
			{
				datasetSet.remove(dataset);
			}
		}
		if (!datasetSet.isEmpty())
		{
			fail("Some data was missing");
		}
	}
}
