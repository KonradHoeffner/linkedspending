package org.aksw.linkedspending;

import java.io.File;

public class Files
{
	static public final File JSON_FOLDER = new File("json");
	static private final File PARTS_FOLDER = new File(JSON_FOLDER,"parts");

	static public File partsSubFolder(String datasetName)
	{
		return new File(PARTS_FOLDER,datasetName);
	}

	static public File datasetJsonFile(String datasetName)
	{
		return new File(JSON_FOLDER,datasetName+".json");
	}
}