package org.aksw.linkedspending;

import org.aksw.linkedspending.tools.PropertiesLoader;
import org.junit.Assert;
import org.junit.Test;
import java.io.File;
import java.util.Properties;


/**
 * Integration Test for the downloader class
 */
public class JsonDownloaderIntegrationTest
{
    private static final Properties PROPERTIES = PropertiesLoader.getProperties("environmentVariables.properties");
    private Scheduler scheduler = new Scheduler();
    private long size;
    private long sizeNew;
    private File downloadDir = new File(PROPERTIES.getProperty("pathParts"));
    private boolean same;


    @Test
    public void downloaderTest()
    {
        size = folderSize(downloadDir);

        scheduler.runDownloader();

        try
        {
            Thread.sleep(30000);
        }
        catch(InterruptedException e)
        {

        }

        scheduler.pauseDownloader();

        sizeNew = folderSize(downloadDir);

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
