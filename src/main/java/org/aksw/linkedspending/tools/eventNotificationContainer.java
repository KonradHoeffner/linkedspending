package org.aksw.linkedspending.tools;

import java.util.Vector;

/** Holds eventNotifications and can create statistical information */
public class eventNotificationContainer
{
    private Vector<eventNotification> notifications;

    public eventNotificationContainer() {notifications = new Vector<>();}

    public Vector<eventNotification> getEventNotifications() {return notifications;}

    /** Returns number of notifications of specified type caused by a certain module.
     * Returns -1 if specified type or module is not valid. */
    public int getTypeCount(byte type, byte causedBy)
    {
        //if(causedBy >= 2 || type >= 11) return -1;
        int count = 0;
        for(eventNotification n : notifications) { if(n.getType() == type && n.getCausedBy() == causedBy) count++;}
        return count;
    }

    /**
     * Looks up whether a certain event occured or not.
     * @param ty Type of event to be looked up
     * @param cB Module of event
     * @return True: event occured, false: event didn't occur
     */
    public boolean checkForEvent(int ty, int cB)
    {
        for(eventNotification n : notifications)
        {
            if(n.getCausedBy() == cB && n.getType() == ty) return true;
        }
        return false;
    }
}