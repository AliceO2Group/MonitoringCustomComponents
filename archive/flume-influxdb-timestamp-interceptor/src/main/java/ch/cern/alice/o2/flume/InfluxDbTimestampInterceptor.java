package ch.cern.alice.o2.flume;

import java.nio.charset.Charset;
import java.util.List;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

/**
 * This interceptor is based on Flume TimestampInterceptor
 * https://github.com/apache/flume/blob/trunk/flume-ng-core/src
 * /main/java/org/apache/flume/interceptor/TimestampInterceptor.java
 * 
 * This component adds a timestamp tag to a influxdb line protocol format event.
 * This timestamp default tag is "timestamp" but could be changed using the configuration file 
 * and its format is a string long timestamp in milliseconds since the UNIX epoch 
 * with a nanosecond accurancy in accordance with InfluxDB timestamp.
 * @author Gioacchino Vino
 */
public class InfluxDbTimestampInterceptor implements Interceptor {

  private String sTagName;
  
  InfluxDbTimestampInterceptor(String sNewTagName) {
    this.sTagName = sNewTagName;
  }
  
  public void initialize() {
    // no-op
  }

  /**
   * Modifies events in-place.
   */
  public Event intercept(Event event) {
    long now = System.currentTimeMillis()*1000000;
    String sBody = new String ( event.getBody(), Charset.forName("UTF-8"));
    String [] sFields = sBody.split(" ");
    sFields[0] += ","+this.sTagName+"="+Long.toString(now);
    String newBody = String.join(" ", sFields);
    event.setBody(newBody.getBytes(Charset.forName("UTF-8")));
    return event;
  }

  /**
   * Delegates to {@link #intercept(Event)} in a loop.
   * @param events
   * @return
   */
  public List<Event> intercept(List<Event> events) {
    for (Event event : events) {
      intercept(event);
    }
    return events;
  }

  public void close() {
    // no-op
  }

  /**
   * Builder which builds new instances of the TimestampInterceptor.
   */
  public static class Builder implements Interceptor.Builder {

    private String sTagName = "timestamp";
    public Interceptor build() {
      return new InfluxDbTimestampInterceptor(sTagName);
    }
    public void configure(Context context) {
      sTagName = context.getString("timestampTag", "timestamp");
    }
  }
}
