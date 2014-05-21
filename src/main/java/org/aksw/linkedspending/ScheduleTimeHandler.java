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

    String startTime = PROPERTIES.getProperty("startAtHour");
    String startDay = PROPERTIES.getProperty("startAtDay");
    String repeatEveryXWeeks = PROPERTIES.getProperty("repeatStartEveryXWeeks");

    /** Sets shutdownRequested to value of param setTo
     * @param setTo the desired value of shutdownRequested */
    public static void setShutdownRequested(boolean setTo) {shutdownRequested = setTo;}

    @Override
    public void run()
    {
        //String givenTime = PROPERTIES.getProperty("startAtHour");
        //System.out.println(startTime);

        while(!shutdownRequested)
        {
            Calendar cal = Calendar.getInstance();
            //do we have reached the day to start?
            if(startToday() || startInLessThanADay())
            {
                Date date = new Date(System.currentTimeMillis());

                while(!hour.format(date).equals(startTime) && !shutdownRequested)
                {
                    try{Thread.sleep(60000);}
                    catch(InterruptedException e)
                    {
                        e.printStackTrace();
                        continue;
                    }
                    date = new Date(System.currentTimeMillis());
                }
                if(!shutdownRequested)
                {
                    Scheduler.scheduleCompleteRun(1,"s");
                    sleepTillNextRun();
                }
            }
            else    // wait one day
            {
                int i = 1000*60*60*24;
                while(i>0 && !shutdownRequested)
                {
                    try
                    {
                        Thread.sleep(120000);
                        i-=120000;
                    }
                    catch(InterruptedException e)
                    {
                        e.printStackTrace();
                        continue;
                    }
                }

            }
        }
    }

    /** Once a run has been started, this method puts the Handler asleep until it should the next run.
     * The time to be sleeping is calculated by getting the repeatStartEveryXWeeks environment variable. */
    private void sleepTillNextRun()
    {
        Integer repeat = new Integer(repeatEveryXWeeks);
        long sleepTime;
        sleepTime = repeat*7*24*60*60*1000 - 5*60000; //604.8 million ms for one week
        while(!shutdownRequested && sleepTime>0)
        {
            try
            {
                Thread.sleep(120000);
                sleepTime-=120000;
            }
            catch(InterruptedException e) {e.printStackTrace();}
        }
    }

    /** Returns true, if the specified start hour is less than the current hour (aka is less than 24h in the
     * future. Returns false, if the specified start hour is greater than the current hour (aka is more than
     * 24h in the future */
    private boolean startInLessThanADay()
    {
        Integer todaysDay;
        Integer startDay;

        Integer x = new Integer(startTime);
        Integer y = new Integer(hour.format(new Date(System.currentTimeMillis())));

        Calendar cal = Calendar.getInstance();
        todaysDay = new Integer(day.format(cal.getTimeInMillis()));
        startDay = new Integer(this.startDay);

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
        if(day.equals(startDay)) return true;
        else return false;
    }

    /** Sets hour to start to another than default value.
     * @param hour Desired start time */
    public void setStartTime(String hour) { startTime = hour; }

    /** Sets day to start to another than default value.
     * @param day Desired start day. 1 = Monday, ... , 7 = Sunday */
    public void setStartDay(String day) { startDay = day; }

    /** Sets hour to start to another than default value.
     * @param hour Desired start time */
    public void setStartTime(int hour)
    {
        String hourString = new String();
        hourString+= hour;
        startTime = hourString;
    }

    /** Sets day to start to another than default value.
     * @param day Desired start day. 1 = Monday, ... , 7 = Sunday */
    public void setStartDay(int day)
    {
        String dayString = new String();
        dayString+= hour;
        startDay = dayString;
    }

    /** Sets ratio to repeat runs. E.g., value = 2 means it will repeat every 2 weeks. */
    public void setRepeat(String value) {repeatEveryXWeeks = value;}

    /** Sets ratio to repeat runs. E.g., value = 2 means it will repeat every 2 weeks. */
    public void setRepeat(int value)
    {
        String repeatString = new String();
        repeatString+=value;
        repeatEveryXWeeks = repeatString;
    }
}
