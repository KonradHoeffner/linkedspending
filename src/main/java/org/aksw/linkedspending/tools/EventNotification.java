package org.aksw.linkedspending.tools;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/** Class to support a common format of EventNotification usable by both Downloader and Converter
 * (and other modules as well) which can also be used to create some statistical information. */
public class EventNotification
{
    /* Feel free to add more constants if needed */
    /* Type constants */

    //Todo: rework
    public static final byte finishedDownloadingSingle = 0;
    public static final byte finishedDownloadingComplete = 1;
    public static final byte finishedConvertingSingle = 2;
    public static final byte finishedConvertingComplete = 3;
    public static final byte fileNotFound = 4;
    public static final byte unsupportedFileType = 5;
    public static final byte outOfMemory = 6;
    public static final byte startedDownloadingSingle = 7;
    public static final byte startedDownloadingComplete = 8;
    public static final byte startedConvertingSingle = 9;
    public static final byte startedConvertingComplete = 10;
    public static final byte downloadStopped = 11;
    public static final byte downloadPaused = 12;
    public static final byte downloadResumed = 13;
    public static final byte tooManyErrors = 14;
    public static final byte runTimeError = 15;
    public static final byte stoppedConverter = 16;
    public static final byte pausedConverter = 17;
    public static final byte resumedConverter = 18;

    /* causedBy constants */
    public static final byte causedByConverter = 0;
    public static final byte causedByDownloader = 1;

    private long time;
    private byte type;
    private byte causedBy;

    //todo please write what this is for
    /** Not to be used for all events (only makes sense with types 0, 1, 2, 3 */
    private boolean success;

    /** Creates new EventNotification.
     * @param type the type of Event to be created<br>finishedDownloadingSingle = 0<br>finishedDownloadingComplete = 1<br>finishedConvertingSingle = 2<br>fileNotFound = 4<br>
     *           unsupportedFileType = 5<br>outOfMemory = 6<br>startedDownloadingSingle = 7<br>startedDownloadingComplete = 8<br>
     *           startedConvertingSingle = 9<br>startedConvertingComplete = 10<br>downloadStopped = 11<br>downloadPaused = 12<br>
     *           downloadResumed = 13<p>
     * @param causedBy by what softwaremodul the event is caused<br>Converter = 0<br>Downloader = 1
     */
    public EventNotification(int type, int causedBy)
    {
        time = System.currentTimeMillis();
        type = (byte) type;
        causedBy = (byte) causedBy;
    }

    /** Creates new EventNotification.
     * @param type the type of Event to be created<br>finishedDownloadingSingle = 0<br>finishedDownloadingComplete = 1<br>finishedConvertingSingle = 2<br>fileNotFound = 4<br>
     *           unsupportedFileType = 5<br>outOfMemory = 6<br>startedDownloadingSingle = 7<br>startedDownloadingComplete = 8<br>
     *           startedConvertingSingle = 9<br>startedConvertingComplete = 10<br>downloadStopped = 11<br>downloadPaused = 12<br>
     *           downloadResumed = 13<p>
     * @param causedBy by what softwaremodul the event is caused<br>Converter = 0<br>Downloader = 1
     */
    public EventNotification(int type, int causedBy, boolean success)
    {
        time = System.currentTimeMillis();
        type = (byte) type;
        causedBy = (byte) causedBy;
        this.success = success;
    }

    public long getTime() {return time;}

    public int getType() {return (int)type;}

    public int getCausedBy() {return (int)causedBy;}

    /** Returns a String of following format: "time causedBy type" (for withTime = true) or "causedBy type" (withTime = false) */
    public String getEventCode(boolean withTime)
    {
        String s, t;
        DateFormat dF = new SimpleDateFormat("HH:mm.ss");
        t = dF.format(time);
        if (withTime) s = t + " " + causedBy + " " + type;
        else s = causedBy + " " + type;
        return s;
    }

    /** Returns event as string. */
    public String getEvent()
    {
        String s = new String();
        switch (type)
        {
            case 0 : s = "finishedDownloadingSingle";
            case 1 : s = "finishedDownloadingComplete";
            case 2 : s = "finishedConvertingSingle";
            case 3 : s = "finishedConvertingComplete";
            case 4 : s = "fileNotFound";
            case 5 : s = "unsupportedFileType";
            case 6 : s = "outOfMemory";
            case 7 : s = "startedDownloadingSingle";
            case 8 : s = "startedDownloadingComplete";
            case 9 : s = "startedConvertingSingle";
            case 10 : s = "startedConvertingComplete";
            case 11 : s = "downloadStopped";
            case 12 : s = "downloadPaused";
            case 13 : s = "downloadResumed";
            case 14 : s = "tooManyErrors";
            case 15 : s = "runTimeError";
            case 16 : s = "stoppedConverter";
            case 17 : s = "pausedConverter";
            case 18 : s = "resumedConverter";
        }
        return s;
    }
}