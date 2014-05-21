package org.aksw.linkedspending.tools;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Vector;

/** Holds eventNotifications and can create statistical information */
public class EventNotificationContainer
{
    /** A vector containing eventNotifications */
    private Vector<EventNotification> notifications;

    public EventNotificationContainer() {notifications = new Vector<>();}

    /** @return Vector<EventNotification> notifications */
    public Vector<EventNotification> getEventNotifications() {return notifications;}

    /** Returns number of notifications of specified type caused by a certain module.
     * Returns -1 if specified type or module is not valid. */
    public int getTypeCount(EventNotification.EventType type, EventNotification.EventSource source)
    {
        //if(causedBy >= 2 || type >= 11) return -1;
        int count = 0;
        for(EventNotification n : notifications) { if(n.getType() == type && n.getSource() == source) count++;}
        return count;
    }

    /**
     * Looks up whether a certain event occured or not.
     * @param type Type of event to be looked up
     * @param source Module of event
     * @return True: event occured, false: event didn't occur
     */
    public boolean checkForEvent(EventNotification.EventType type, EventNotification.EventSource source)
    {
        for(EventNotification n : notifications)
        {
            if(n.getSource() == source && n.getType() == type) return true;
        }
        return false;
    }

    /** Prints all occured events into a file named eventOutput. If this file already exists, output will be printed
     * into a file named eventOutput1 (or any number, in case there more of these files) */
    public void printEventsToFile()
    {
        try
        {
            File f = new File("eventOutput");
            int i = 1;
            String fileName = "eventOutput";
            while(true)
            {
                if(f.exists())
                {
                    f = null;
                    f = new File("eventOutput"+i);
                }
                else
                {
                    FileWriter output = new FileWriter(f);
                    for(int j=0; i < notifications.size(); i++)
                    {
                        output.write(notifications.get(j).getEventCode(true));
                        output.append(System.getProperty("line.separator"));
                    }
                    output.close();
                    break;
                }
                i++;
            }
        }
        catch(IOException e) { e.printStackTrace(); }
    }

    /** Removes all elements contained in notifications vector */
    public void clear()
    {
        notifications.clear();
    }

    /** Adds a new EventNotification to notifications vector
     * @param e EventNotification to be added */
    public void add(EventNotification e)
    {
        notifications.add(e);
    }
}