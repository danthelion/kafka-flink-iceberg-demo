package com.arecadata.clickstream.clicks;

import org.apache.flink.types.Row;

import java.util.Objects;

/**
 * A simple Event that Flink recognizes as a valid POJO.
 *
 * <p>Because it has valid (deterministic) implementations of equals and hashcode, this type can
 * also be used a key.
 */
public class Click {

    /** A Flink POJO must have public fields, or getters and setters */
    public String timestamp;
    public String event;
    public String user_id;
    public String site_id;
    public String url;
    public Integer on_site_seconds;
    public Integer viewed_percent;

    /** A Flink POJO must have a no-args default constructor */
    public Click() {}

    public Click(String timestamp, String event, String user_id, String site_id, String url, Integer on_site_seconds, Integer viewed_percent) {
        this.timestamp = timestamp;
        this.event = event;
        this.user_id = user_id;
        this.site_id = site_id;
        this.url = url;
        this.on_site_seconds = on_site_seconds;
        this.viewed_percent = viewed_percent;
    }

    /** Used for printing during development */
    @Override
    public String toString() {
        return "Click{" +
                "timestamp=" + timestamp +
                ", event='" + event + '\'' +
                ", user_id='" + user_id + '\'' +
                ", site_id='" + site_id + '\'' +
                ", url='" + url + '\'' +
                ", on_site_seconds=" + on_site_seconds +
                ", viewed_percent=" + viewed_percent +
                '}';
    }

    public Row toRow() {
        return Row.of(timestamp, event, user_id, site_id, url, on_site_seconds, viewed_percent);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Click click = (Click) o;
        return event.equals(click.event) && user_id.equals(click.user_id) && timestamp.equals(click.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(event, user_id, timestamp);
    }
}