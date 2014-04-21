package org.aksw.linkedspending;


import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Converter {

    /**
     * Map for all files to be loaded into the Converter
     */
    static Map<String,File> files = new ConcurrentHashMap<>();




    /**
     * Gets a file that is already provided by JSON-Downloader to be converted into rdf. Uses ConcurrentHashMap to track all files in Converter.
     * @param name the name of the JSON-file(LS JSON Diff)
     * @return the file to be converted into the tripple-store
     */
	static File getDatasetFile(String name)
	{
		File file = files.get(name);
		if(file==null) files.put(name,file= new File(Main.folder+"/"+name+".nt"));
		return file;
	}
}
