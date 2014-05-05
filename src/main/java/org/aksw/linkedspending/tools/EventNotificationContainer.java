package org.aksw.linkedspending.tools;

import java.util.Vector;

/** Holds eventNotifications and can create statistical information */
public class EventNotificationContainer
{
    private Vector<EventNotification> notifications;

    public EventNotificationContainer() {notifications = new Vector<>();}

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
}