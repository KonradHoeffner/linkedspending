package org.aksw.linkedspending;

import java.io.File;

public class DataSetFiles
{
	static public final File JSON_FOLDER = new File("json");
	static public final File RDF_FOLDER = new File("output");

	static private final File PARTS_FOLDER = new File(JSON_FOLDER,"parts");

	static public File partsSubFolder(String datasetName, int pageSize)
	{
		return new File(PARTS_FOLDER,datasetName+"-"+pageSize);
	}

	static public File datasetJsonFile(String datasetName)
	{
		return new File(JSON_FOLDER,datasetName+".json");
	}
}