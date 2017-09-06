package org.ladbury.mqqtListener;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

public class TimestampedDouble
{
    private double value;
    private Instant timestamp;

    public TimestampedDouble()
    {
        value = 0;
        timestamp = Instant.now();
    }
    public TimestampedDouble(double value)
    {
        this.value = value;
        this.timestamp = Instant.now();
    }
    public TimestampedDouble(double value, Instant timestamp)
    {
        this.value = value;
        this.timestamp = timestamp;
    }
    public TimestampedDouble(double value, String timeString)
    {
        this.value = value;
        TemporalAccessor creationAccessor = DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(timeString);
        this.timestamp = Instant.from(creationAccessor);
    }

    public Instant getTimestamp()
    {
        return timestamp;
    }
    public void setTimestamp(Instant timestamp)
    {
        this.timestamp = timestamp;
    }
    public double getValue()
    {
        return value;
    }
    public void setValue(double value)
    {
        this.value = value;
    }
}
