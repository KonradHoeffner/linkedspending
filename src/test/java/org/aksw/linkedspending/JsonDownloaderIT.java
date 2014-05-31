package org.aksw.linkedspending;

import org.aksw.linkedspending.tools.PropertiesLoader;
import org.junit.Test;
import java.io.File;
import java.util.Properties;

import static org.junit.Assert.fail;


/**
 * Integration Test for the downloader class
 */
public class JsonDownloaderIT
{
    private static final Properties PROPERTIES = PropertiesLoader.getProperties("environmentVariables.properties");
    private int number,numberNew,numberParts,numberPartsNew;
    private File downloadDir = new File(PROPERTIES.getProperty("pathJson"));
    private File partsDir = new File(PROPERTIES.getProperty("pathParts"));

    @Test
    public void downloaderTest()
    {
        number = fileNumber(downloadDir, false);
        numberParts = fileNumber(partsDir, true);

        Scheduler.runManually();
        Scheduler.runDownloader();

        try
        {
            Thread.sleep(30000);
        }
        catch(InterruptedException e)
        {
            fail("Interrupted exception: " + e.getMessage());
        }

        Scheduler.stopDownloader();

        numberNew = fileNumber(downloadDir, false);
        numberPartsNew = fileNumber(partsDir, true);

        System.out.println(number + " " + numberNew);
        System.out.println(numberParts + " " + numberPartsNew);

        if(number < numberNew)
        {
            fail("Number of files decreased");
        }
        else if (number == numberNew)
        {
            if(numberParts == numberPartsNew)
            {
                fail("Number of files not increased");
            }
        }
    }

    private int fileNumber(File directory, boolean recursive)
    {
        int number = 0;
        for (File file : directory.listFiles())
        {
            if (file.isFile())
            {
                number++;
            }
            else if(recursive)
            {
                number += fileNumber(file, true);
            }
        }
        return number;
    }
}
