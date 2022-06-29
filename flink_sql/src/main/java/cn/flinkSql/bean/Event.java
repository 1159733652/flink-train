package cn.flinkSql.bean;

/**
 * @Author zhanghongyu
 * @Date 2022/6/29
 * @DESC    事件实体类
 */
public class Event {
    //{"guid":1,"eventId":"e01","eventTime":"2022-06-12 14:35:19.200","pageId":"p001"}
    public int guid;
    public String eventId;
    public long eventTime;
    public String pageId;

    public Event() {
    }

    public Event(int guid, String eventId, long eventTime, String pageId) {
        this.guid = guid;
        this.eventId = eventId;
        this.eventTime = eventTime;
        this.pageId = pageId;
    }

    public int getGuid() {
        return guid;
    }

    public void setGuid(int guid) {
        this.guid = guid;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public long getEventTime() {
        return eventTime;
    }

    public void setEventTime(long eventTime) {
        this.eventTime = eventTime;
    }

    public String getPageId() {
        return pageId;
    }

    public void setPageId(String pageId) {
        this.pageId = pageId;
    }

    @Override
    public String toString() {
        return "Event{" +
                "guid=" + guid +
                ", eventId='" + eventId + '\'' +
                ", eventTime=" + eventTime +
                ", pageId='" + pageId + '\'' +
                '}';
    }
}
