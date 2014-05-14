package org.aksw.linkedspending.tools;

import org.aksw.linkedspending.Scheduler;

/** Puts Converter thread asleep while downloading has not been finished. */
public class ConverterSleeper extends Scheduler implements Runnable
{
    @Override
    public void run()
    {
        super.getConverterThread().start();

        while(!super.getDownloader().getEventContainer().checkForEvent(EventNotification.EventType.finishedDownloadingComplete, EventNotification.EventSource.Downloader))
        { //condition might cause bug when multiple events of this type occur
            try {super.getConverterThread().sleep(15000);}
            catch(InterruptedException e) {}
        }
    }
}