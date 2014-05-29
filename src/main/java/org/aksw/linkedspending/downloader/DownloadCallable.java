package org.aksw.linkedspending.downloader;

import lombok.extern.java.Log;
import org.aksw.linkedspending.OpenspendingSoftwareModul;
import org.aksw.linkedspending.Scheduler;
import org.eclipse.jdt.annotation.Nullable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;

import static org.aksw.linkedspending.downloader.HttpConnectionUtil.getConnection;

/**Implements the logic for downloading a JSON-file within a thread. Is similar to the use of the Runnable Interface, but its call method can give a return value.<p>
 * If the dataset has no more than PAGE_SIZE results, it gets saved to json/datasetName, else it gets split into parts
 * in the folder json/parts/pagesize/datasetname with filenames datasetname.0, datasetname.1, ... , datasetname.final **/
@Log
class DownloadCallable implements Callable<Boolean>
{
    /**name of the dataset to be downloaded*/
    final String datasetName;
    //        final URL entries;
    /**id for the Instance*/
    final int nr;
    //        int pageSize;

    /**
     * normal constructor
     * @param datasetName the name of the dataset to be downloaded
     * @param nr the id for this instance
     * @throws java.net.MalformedURLException
     */
    DownloadCallable(String datasetName, int nr) throws MalformedURLException
    {
        this.datasetName = datasetName;
        this.nr=nr;
        //            this.pageSize=pageSize;
        //            entries = new URL("http://openspending.org/"+datasetName+"/entries.json?pagesize="+PAGE_SIZE);
    }

    /**
     * implements the real logic for downloading a file from openspending
     * @return true if dataset was completely downloaded, false otherwise
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override public @Nullable
    Boolean call() throws IOException, InterruptedException
    {
        Path path = Paths.get(OpenspendingSoftwareModul.pathJson.getPath(), datasetName);
        File file = path.toFile();
        File partsFolder = new File(OpenspendingSoftwareModul.pathJson.toString()+"/parts/"+datasetName);
        File finalPart = new File(partsFolder.toString()+"/"+datasetName+".final");
        //            Path partsPath = Paths.get(partsFolder.getPath(),datasetName);
        log.fine(nr + " Fetching number of entries for dataset " + datasetName);

        //here is where all the readJSON... stuff is exclusively used
        int nrEntries = OpenspendingSoftwareModul.nrEntries(datasetName);
        if(nrEntries==0)
        {
            log.fine(nr+" No entries for dataset "+datasetName+" skipping download.");
            JsonDownloader.emptyDatasets.add(datasetName);
            synchronized(JsonDownloader.emptyDatasets)
            {
                try(ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(JsonDownloader.emptyDatasetFile)))
                {
                    log.fine(nr+" serializing "+ JsonDownloader.emptyDatasets.size()+" entries to file");
                    out.writeObject(JsonDownloader.emptyDatasets);
                }
            }
            // save as empty file to make it faster? but then it slows down normal use
            return false;
        }
        log.info(nr+" Starting download of "+datasetName+", "+nrEntries+" entries.");
        int nrOfPages = (int)(Math.ceil((double)nrEntries/ JsonDownloader.pageSize));

        partsFolder.mkdirs();
        // starts from beginning when final file already exists
        File finalFile = new File(partsFolder.toString() + "/" + datasetName + ".final");
        if(finalFile.exists())
        {
            for (File part : partsFolder.listFiles())
            {
                part.delete();
            }
        }
        for(int page=1;page<=nrOfPages;page++)
        {
            while(Scheduler.getDownloader().getPauseRequested()) //added to make downloader pausable
            {
                try {Thread.sleep(5000);}
                catch(InterruptedException e) {}
            }

            File f = new File(partsFolder.toString()+"/"+datasetName+"."+(page==nrOfPages?"final":page));
            if(f.exists()) {continue;}
            log.fine(nr+" page "+page+"/"+nrOfPages);
            URL entries = new URL("https://openspending.org/"+datasetName+"/entries.json?pagesize="+ JsonDownloader.pageSize+"&page="+page);
            //                System.out.println(entries);

            if(Scheduler.getDownloader().getStopRequested())
            {
                System.out.println("Aborting DownloadCallable");
                Scheduler.getDownloader().getUnfinishedDatasets().add(datasetName);
                return false;
            }

            try
            {
                HttpURLConnection connection = getConnection(entries);
            }
            catch (HttpConnectionUtil.HttpTimeoutException | HttpConnectionUtil.HttpUnavailableException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            ReadableByteChannel rbc = Channels.newChannel(entries.openStream());
            try(FileOutputStream fos = new FileOutputStream(f))
            {fos.getChannel().transferFrom(rbc, 0, Integer.MAX_VALUE);}
            // ideally, memory should be measured during the transfer but thats not easily possible except
            // by creating another thread which is overkill. Because it is multithreaded anyways I hope this value isn't too far from the truth.
            JsonDownloader.memoryBenchmark.updateAndGetMaxMemoryBytes();
        }
        // TODO: sometimes at the end "]}" is missing, add it in this case
        // manually solvable in terminal with cat /tmp/problems  | xargs -I  @  sh -c "echo ']}' >> '@'"
        // where /tmp/problems is the file containing the list of files with the error
        log.info(nr+" Finished download of "+datasetName+".");
        Scheduler.getDownloader().getFinishedDatasets().add(datasetName);
        return true;
    }
}
