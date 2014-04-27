package org.aksw.linkedspending.tools;

/** Class to support a common format of eventNotification usable by both Downloader and Converter
 * (and other modules as well) which can also be used to create some statistical information. */
public class eventNotification
{
    /* Feel free to add more constants if needed */
    /* Type constants */
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

    /* causedBy constants */
    public static final byte causedByConverter = 0;
    public static final byte causedByDownloader = 1;

    private long time;
    private byte type;
    private byte causedBy;

    public eventNotification(int ty, int cB)
    {
        time = System.currentTimeMillis();
        type = (byte) ty;
        causedBy = (byte) cB;
    }

    public long getTime() {return time;}

    public int getType() {return (int)type;}

    public int getCausedBy() {return (int)causedBy;}

    /** Returns a String of following format: "time causedBy type" (for withTime = false) or "causedBy type" (withTime = true) */
    public String getEventCode(boolean withTime) {
        String s;
        if (withTime) s = time + " " + causedBy + " " + type;
        else s = causedBy + " " + type;
        return s;
    }
}