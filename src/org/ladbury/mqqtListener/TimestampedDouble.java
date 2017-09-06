package org.ladbury.mqqtListener;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

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
        TemporalAccessor creationAccessor = DateTimeFormatter.ISO_ZONED_DATE_TIME.parse(timeString);
        this.timestamp = Instant.from(creationAccessor);
    }

    public String toString()
    {
        final DateTimeFormatter formatter =
                DateTimeFormatter.ISO_OFFSET_DATE_TIME
                        .withLocale( Locale.UK )
                        .withZone( ZoneId.systemDefault() );
        //System.out.println(String.format("Metric: {%.03f %s at %s}", value,unit.getSymbol(), formatter.format(timestamp)));
        return String.format("%.03f at %s", value, formatter.format(timestamp));
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
