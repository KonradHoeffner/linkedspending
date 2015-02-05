package org.aksw.linkedspending.tools;

import java.util.Scanner;
import org.aksw.linkedspending.LinkedSpendingDatasetInfo;
import org.aksw.linkedspending.Virtuoso;

public class DeleteAllSubGraphs
{

	@SuppressWarnings("resource") public static void main(String[] args)
	{
		System.out.println("type 'delete' to delete all subgraphs from virtuoso.");
		String line = new Scanner(System.in).nextLine();
		if(!line.equals("delete")) {System.out.println("aborted.");System.exit(0);}
		else
		{
			System.out.println("deleting");
			for(String datasetName: LinkedSpendingDatasetInfo.cached().keySet()) {Virtuoso.deleteSubGraph(datasetName);}
		}
	}

}
