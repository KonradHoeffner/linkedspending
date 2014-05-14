package org.aksw.linkedspending;

import org.aksw.linkedspending.tools.PropertiesLoader;
import org.junit.Assert;
import org.junit.Test;
import java.io.File;
import java.util.Properties;

import static org.junit.Assert.fail;


/**
 * Integration Test for the downloader class
 */
public class JsonDownloaderIntegrationTest
{
    private static final Properties PROPERTIES = PropertiesLoader.getProperties("environmentVariables.properties");
    private Scheduler scheduler = new Scheduler();
    private int number,numberNew,numberParts,numberPartsNew;
    private File downloadDir = new File(PROPERTIES.getProperty("pathJson"));
    private File partsDir = new File(PROPERTIES.getProperty("pathParts"));

    @Test
    public void downloaderTest()
    {
        number = fileNumber(downloadDir);
        numberParts = fileNumberRec(partsDir);

        scheduler.runDownloader();

        try
        {
            Thread.sleep(30000);
        }
        catch(InterruptedException e)
        {
            fail("Interrupted exception: " + e.getMessage());
        }

        scheduler.pauseDownloader();

        numberNew = fileNumber(downloadDir);
        numberPartsNew = fileNumberRec(partsDir);

        //System.out.println(number + " " + numberNew);
        //System.out.println(numberParts + " " + numberPartsNew);

        if(number < numberNew)
        {
            fail("Number of files decreased");
        }
        else if (number == numberNew)
        {
            if(numberParts < numberPartsNew){}
            else
            {
                fail("Number of files not increased");
            }
        }
    }

    private int fileNumberRec(File directory)
    {
        int number = 0;
        for (File file : directory.listFiles()) {
            if (file.isFile())
            {
                number++;
            }
            else
            {
                number += fileNumberRec(file);
            }
        }
        return number;
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
