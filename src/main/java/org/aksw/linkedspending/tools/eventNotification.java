package org.aksw.linkedspending.tools;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/** Class to support a common format of eventNotification usable by both Downloader and Converter
 * (and other modules as well) which can also be used to create some statistical information. */
public class eventNotification
{
    /* Feel free to add more constants if needed */
    /* Type constants */
    //Todo: add notifications for unmarked (//) constants
    public static final byte finishedDownloadingSingle = 0;     //
    public static final byte finishedDownloadingComplete = 1;   //
    public static final byte finishedConvertingSingle = 2;
    public static final byte finishedConvertingComplete = 3;
    public static final byte fileNotFound = 4;
    public static final byte unsupportedFileType = 5;
    public static final byte outOfMemory = 6;
    public static final byte startedDownloadingSingle = 7;      //
    public static final byte startedDownloadingComplete = 8;    //
    public static final byte startedConvertingSingle = 9;
    public static final byte startedConvertingComplete = 10;
    public static final byte downloadStopped = 11;              //
    public static final byte downloadPaused = 12;               //
    public static final byte downloadResumed = 13;              //

    /* causedBy constants */
    public static final byte causedByConverter = 0;
    public static final byte causedByDownloader = 1;

    private long time;
    private byte type;
    private byte causedBy;
    /** Not to be used for all events (only makes sense with types 0, 1, 2, 3 */
    private boolean success;

    /** Creates new eventNotification.
     * @param ty finishedDownloadingSingle = 0, finishedDownloadingComplete = 1, finishedConvertingSingle = 2, fileNotFound = 4,
     *           unsupportedFileType = 5, outOfMemory = 6, startedDownloadingSingle = 7, startedDownloadingComplete = 8,
     *           startedConvertingSingle = 9, startedConvertingComplete = 10, downloadStopped = 11, downloadPaused = 12,
     *           downloadResumed = 13
     * @param cB Converter = 0, Downloader = 1
     */
    public eventNotification(int ty, int cB)
    {
        time = System.currentTimeMillis();
        type = (byte) ty;
        causedBy = (byte) cB;
    }

    /** Creates new eventNotification.
     * @param ty finishedDownloadingSingle = 0, finishedDownloadingComplete = 1, finishedConvertingSingle = 2, fileNotFound = 4,
     *           unsupportedFileType = 5, outOfMemory = 6, startedDownloadingSingle = 7, startedDownloadingComplete = 8,
     *           startedConvertingSingle = 9, startedConvertingComplete = 10, downloadStopped = 11, downloadPaused = 12,
     *           downloadResumed = 13
     * @param cB Converter = 0, Downloader = 1
     */
    public eventNotification(int ty, int cB, boolean success)
    {
        time = System.currentTimeMillis();
        type = (byte) ty;
        causedBy = (byte) cB;
        this.success = success;
    }

    public long getTime() {return time;}

    public int getType() {return (int)type;}

    public int getCausedBy() {return (int)causedBy;}

    /** Returns a String of following format: "time causedBy type" (for withTime = false) or "causedBy type" (withTime = true) */
    public String getEventCode(boolean withTime) {
        String s, t;
        DateFormat dF = new SimpleDateFormat("HH:mm.ss");
        t = dF.format(time);
        if (withTime) s = t + " " + causedBy + " " + type;
        else s = causedBy + " " + type;
        return s;
    }
}