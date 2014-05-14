package org.aksw.linkedspending;

import org.aksw.linkedspending.tools.PropertiesLoader;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Properties;

/**
 * Integration Test for the converter class
 */
public class ConverterIntegrationTest
{
    private static final Properties PROPERTIES = PropertiesLoader.getProperties("environmentVariables.properties");
    private Scheduler scheduler = new Scheduler();
    private long size;
    private long sizeNew;
    private File convertDir = new File(PROPERTIES.getProperty("pathRdf"));
    private boolean same;


    @Test
    public void converterTest()
    {
        size = folderSize(convertDir);

        scheduler.runConverter();

        try
        {
            Thread.sleep(30000);
        }
        catch(InterruptedException e)
        {

        }

        scheduler.stopConverter();

        sizeNew = folderSize(convertDir);

        if(size < sizeNew)
        {
            same = false;
        }
        else
        {
            same = true;
        }

        Assert.assertFalse("Download directory size not increased", same);
    }

    private long folderSize(File directory) {
        long length = 0;
        for (File file : directory.listFiles()) {
            if (file.isFile())
                length += file.length();
            else
                length += folderSize(file);
        }
        return length;
    }

}
