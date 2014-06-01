package org.aksw.linkedspending.tools;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/** Class to support a common format of EventNotification usable by both Downloader and Converter
 * (and other modules as well) which can also be used to create some statistical information. */
public class EventNotification
{
    /* Feel free to add more constants if needed */
    /* Type constants */

    public enum EventType
    {
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
        finishedDownloadingDataset,
        downloadStopped,
        /** pause requested for downloader */
        downloadPaused,
        downloadResumed,
        tooManyErrors,
        runTimeError,
        stoppedConverter,
        pausedConverter,
        resumedConverter,
        IOError
    }

    /* source constants */
    public enum EventSource
    {
        Converter,
        Downloader,
        DownloadCallable
    }

    private long time;
    private EventType type;
    private EventSource source;
    private String note;

    /** Indicates if the event was successful or not.
     * Not to be used for all events (only makes sense with types such as finishedDownloading,...) */
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
        note = null;
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
        note = null;
    }

    /** Creates new EventNotification.
     * @param type the type of Event to be created<br>finishedDownloadingSingle = 0<br>finishedDownloadingComplete = 1<br>finishedConvertingSingle = 2<br>fileNotFound = 4<br>
     *           unsupportedFileType = 5<br>outOfMemory = 6<br>startedDownloadingSingle = 7<br>startedDownloadingComplete = 8<br>
     *           startedConvertingSingle = 9<br>startedConvertingComplete = 10<br>downloadStopped = 11<br>downloadPaused = 12<br>
     *           downloadResumed = 13<p>
     * @param source by what softwaremodul the event is caused<br>Converter = 0<br>Downloader = 1
     * @param note Adds a specified note to the notification (e.g. type=finishedDownloadingDataset, note="berlin_de"
     */
    public EventNotification(EventType type, EventSource source, String note)
    {
        time = System.currentTimeMillis();
        this.type = type;
        this.source = source;
        this.note = note;
    }

    /** Creates new EventNotification.
     * @param type the type of Event to be created<br>finishedDownloadingSingle = 0<br>finishedDownloadingComplete = 1<br>finishedConvertingSingle = 2<br>fileNotFound = 4<br>
     *           unsupportedFileType = 5<br>outOfMemory = 6<br>startedDownloadingSingle = 7<br>startedDownloadingComplete = 8<br>
     *           startedConvertingSingle = 9<br>startedConvertingComplete = 10<br>downloadStopped = 11<br>downloadPaused = 12<br>
     *           downloadResumed = 13<p>
     * @param source by what softwaremodul the event is caused<br>Converter = 0<br>Downloader = 1
     * @param note Adds a specified note to the notification (e.g. type=finishedDownloadingDataset, note="berlin_de"
     * @param success Whether the event was successful or not.
     */
    public EventNotification(EventType type, EventSource source, String note, boolean success)
    {
        time = System.currentTimeMillis();
        this.type = type;
        this.source = source;
        this.note = note;
        this.success = success;
    }

    public long getTime() {return time;}

    public EventType getType() {return type;}

    public EventSource getSource() {return source;}

    /** Returns a String of following format: "time source type" (for withTime = true) or "source type" (withTime = false) */
    public String getEventCode(boolean withTime)
    {
        return (withTime ? new SimpleDateFormat("HH:mm.ss").format(time) + " " : "") +
                source + " " +
                type + " " +
                (note.equals(null) ? "" : note + " ") +
                (success ? "successful" : "unsuccessful");
    }
}