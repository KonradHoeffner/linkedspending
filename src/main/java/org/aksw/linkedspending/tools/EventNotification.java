package org.aksw.linkedspending.tools;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/** Class to support a common format of EventNotification usable by both Downloader and Converter
 * (and other modules as well) which can also be used to create some statistical information. */
public class EventNotification
{
    /* Feel free to add more constants if needed */
    /* Type constants */

    public enum EventType {
        finishedDownloadingSingle,
        finishedDownloadingComplete,
        finishedConvertingSingle,
        finishedConvertingComplete,
        fileNotFound,
        unsupportedFileType,
        outOfMemory,
        startedDownloadingSingle,
        startedDownloadingComplete,
        startedConvertingSingle,
        startedConvertingComplete,
        downloadStopped,
        /** pause requested for downloader */
        downloadPaused,
        downloadResumed,
        tooManyErrors,
        runTimeError,
        stoppedConverter,
        pausedConverter,
        resumedConverter
    }

    /* source constants */
    public enum EventSource {
        Converter,
        Downloader
    }

    private long time;
    private EventType type;
    private EventSource source;

    //todo please write what this is for
    /**
     * Not to be used for all events (only makes sense with types 0, 1, 2, 3 */
    private boolean success;

    /** Creates new EventNotification.
     * @param type the type of Event to be created<br>finishedDownloadingSingle = 0<br>finishedDownloadingComplete = 1<br>finishedConvertingSingle = 2<br>fileNotFound = 4<br>
     *           unsupportedFileType = 5<br>outOfMemory = 6<br>startedDownloadingSingle = 7<br>startedDownloadingComplete = 8<br>
     *           startedConvertingSingle = 9<br>startedConvertingComplete = 10<br>downloadStopped = 11<br>downloadPaused = 12<br>
     *           downloadResumed = 13<p>
     * @param source by what softwaremodul the event is caused<br>Converter = 0<br>Downloader = 1
     */
    public EventNotification(EventType type, EventSource source)
    {
        time = System.currentTimeMillis();
        this.type = type;
        this.source = source;
    }

    /** Creates new EventNotification.
     * @param type the type of Event to be created<br>finishedDownloadingSingle = 0<br>finishedDownloadingComplete = 1<br>finishedConvertingSingle = 2<br>fileNotFound = 4<br>
     *           unsupportedFileType = 5<br>outOfMemory = 6<br>startedDownloadingSingle = 7<br>startedDownloadingComplete = 8<br>
     *           startedConvertingSingle = 9<br>startedConvertingComplete = 10<br>downloadStopped = 11<br>downloadPaused = 12<br>
     *           downloadResumed = 13<p>
     * @param source by what softwaremodul the event is caused<br>Converter = 0<br>Downloader = 1
     */
    public EventNotification(EventType type, EventSource source, boolean success)
    {
        time = System.currentTimeMillis();
        this.type = type;
        this.source = source;
        this.success = success;
    }

    public long getTime() {return time;}

    public EventType getType() {return type;}

    public EventSource getSource() {return source;}

    /** Returns a String of following format: "time source type" (for withTime = true) or "source type" (withTime = false) */
    public String getEventCode(boolean withTime)
    {
        String s, t;
        //We need to format the time, which is internally stored as ms since 1st January 1970
        DateFormat dF = new SimpleDateFormat("HH:mm.ss");
        t = dF.format(time);
        if (withTime) s = t + " " + source + " " + type;
        else s = source + " " + type;
        return s;
    }
}