package org.aksw.linkedspending.tools;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import de.konradhoeffner.commons.TSVReader;

/**
 * Is only used to create data for the scientific publication.
 * Script to generate a gnuplot data file out of multiple outputs of "uniq -c"
 **/
public class CreateHistogram
{

	public static void main(String[] args) throws IOException
	{
		File folder = new File("tmp");
		String[] filenames = { "attributes", "dimensions", "measures" };
		int[][] values = new int[50][filenames.length];
		int maxx = 0;
		for (int i = 0; i < filenames.length; i++)
		{
			try (TSVReader in = new TSVReader(new File(folder + "/" + filenames[i] + "_histogram")))
			{
				while (in.hasNextTokens())
				{
					String[] tokens = in.nextTokens();
					int y = Integer.valueOf(tokens[0]);
					int x = Integer.valueOf(tokens[1]);
					maxx = Math.max(maxx, x);
					values[x][i] = y;
					// System.out.println(Arrays.toString(tokens));
				}
			}
		}
		try (PrintWriter out = new PrintWriter(new File(folder + "/histogram")))
		{
			String title = "n       Attributes      Dimensions      Measures";
			System.out.println(title);
			out.println(title);
			int[] zeroes = { 0, 0, 0 };
			for (int x = 0; x <= maxx; x++)
			{
				if (Arrays.equals(zeroes, values[x]))
				{
					continue;
				}
				String row = x + "    " + (Arrays.toString(values[x]).replaceAll("[\\,\\[\\]]", "").replaceAll("[\\s\t]+", "\t"));
				System.out.println(row);
				if (x == maxx) out.print(row);
				else out.println(row);
			}
		}
	}
}