package org.aksw.linkedspending;

import org.aksw.linkedspending.tools.PropertiesLoader;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

public class ScheduleTimeHandler implements Runnable
{
    private static final Properties PROPERTIES = PropertiesLoader.getProperties("environmentVariables.properties");
    private static boolean shutdownRequested = false;

    private SimpleDateFormat day = new SimpleDateFormat("u");
    private SimpleDateFormat time = new SimpleDateFormat("kk:mm");
    private SimpleDateFormat hour = new SimpleDateFormat("HH");

    /** Sets shutdownRequested to value of param setTo
     * @param setTo the desired value of shutdownRequested
     */
    public void setShutdownRequested(boolean setTo) {shutdownRequested = setTo;}

    @Override
    public void run()
    {
        String givenTime = PROPERTIES.getProperty("startAtHour");
        System.out.println(givenTime);

        while(!shutdownRequested)
        {
            Calendar cal = Calendar.getInstance();
            //do we have reached the day to start?
            if(startToday() || startInLessThanADay())
            {
                System.out.println("Day correct!");
                Date date = new Date(System.currentTimeMillis());

                while(!hour.format(date).equals(givenTime)) {
                    try{Thread.sleep(60000);}
                    catch(InterruptedException e)
                    {
                        e.printStackTrace();
                        continue;
                    }
                    date = new Date(System.currentTimeMillis());
                }
                System.out.println("Hour correct!");
                Scheduler.scheduleCompleteRun(1,"s");
                sleepTillNextRun();
            }
            else
            {
                try{Thread.sleep(1000*60*60*24);}   // wait one day
                catch(InterruptedException e)
                {
                    e.printStackTrace();
                    continue;
                }
            }
        }
    }

    /** Once a run has been started, this method puts the Handler asleep until it should the next run.
     * The time to be sleeping is calculated by getting the repeatStartEveryXWeeks environment variable. */
    private void sleepTillNextRun()
    {
        Integer repeatEveryXWeeks = new Integer(PROPERTIES.getProperty("repeatEveryXWeeks"));
        long sleepTime;
        sleepTime = repeatEveryXWeeks*7*24*60*60*1000 - 5*3600; //604.8 million ms for one week
        try {Thread.sleep(sleepTime);}
        catch(InterruptedException e) {e.printStackTrace();}
    }

    /** Returns true, if the specified start hour is less than the current hour (aka is less than 24h in the
     * future. Returns false, if the specified start hour is greater than the current hour (aka is more than
     * 24h in the future */
    private boolean startInLessThanADay()
    {
        Integer todaysDay;
        Integer startDay;

        Integer x = new Integer(PROPERTIES.getProperty("startAtHour"));
        Integer y = new Integer(hour.format(new Date(System.currentTimeMillis())));

        Calendar cal = Calendar.getInstance();
        todaysDay = new Integer(day.format(cal.getTimeInMillis()));
        startDay = new Integer(PROPERTIES.getProperty("startAtDay"));

        if(todaysDay < startDay && x<=y) return true;
        else return false;
    }

    /** Returns true, if the specified start day is today's day, false otherwise.*/
    private boolean startToday()
    {
        Calendar cal = Calendar.getInstance();
        Integer day;

        if(cal.DAY_OF_WEEK == 7) day = 1;
        else day = cal.DAY_OF_WEEK+1;
        if(day.equals( PROPERTIES.getProperty("startAtDay"))) return true;
        else return false;
    }
}
