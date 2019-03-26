package ch.cern.alice.o2.flume;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;

/**
 * Apache flume InfluxDB HTTP Sink
 * It allows to send line protocol format events to an instance on 
 * InfluxDB via HTTP. 
 * @author Gioacchino Vino
 */
public class InfluxDbHttpSink extends AbstractSink implements Configurable {

  private static final Logger LOG = Logger.getLogger(InfluxDbHttpSink.class);

  /** InfluxDB hostname. */
  private String hostname;
  
  /** InfluxDB port. */
  private int port;
  
  /** InfluxDB database. */
  private String database;
  
  /** Default InfluxDB Hostname */
  private static final String DEFAULT_HOSTNAME = new String("localhost");
  
  /** Default InfluxDB port */
  private static final int DEFAULT_PORT = 8086;
  
  /** Default InfluxDB database */
  private static final String DEFAULT_DATABASE = new String("telegraf"); 
  
  /** Default Batch Size. */
  private static final int DEFAULT_BATCH_SIZE = 500000;
  
  /** Success creation database HTTP status code. */
  private static final int CREATION_DB_OK = 200;
  
  /** Success sending data HTTP status code. */
  private static final int HTTP_STATUS_OK = 204;
  
  /** Lowest HTTP status code for InfluxDB could not understand the request. */
  private static final int HTTP_MIN_4XX = 400;
  
  /** Highest HTTP status code for InfluxDB could not understand the request. */
  private static final int HTTP_MAX_4XX = 499;
  
  /** Lowest HTTP status code for system issues. */
  private static final int HTTP_MIN_5XX = 500;
  
  /** Lowest HTTP status code for system issues. */
  private static final int HTTP_MAX_5XX = 599;
  
  /** Default setting for the connection timeout when calling endpoint. */
  private static final int DEFAULT_CONNECT_TIMEOUT = 10000;

  /** Default setting for the request timeout when calling endpoint. */
  private static final int DEFAULT_REQUEST_TIMEOUT = 10000;

  /** Endpoint URL used to write data to InfluxDB. */
  private URL write_endpointUrl;
  
  /** Endpoint URL used to check the InfluxDB status. */
  private URL pingEndpointUrl;
  
  /** Endpoint URL use to create a database in InfluxDB. */
  private URL cdbEndpointUrl;
  
  /** Counter used to monitor event throughput. */
  private SinkCounter sinkCounter;

  /** Actual batch size. */
  private int batchSize = DEFAULT_BATCH_SIZE;
  
  /** Actual connection timeout value in use. */
  private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;

  /** Actual request timeout value in use. */
  private int requestTimeout = DEFAULT_REQUEST_TIMEOUT;

  /** Used to create HTTP connections to the endpoint. */
  private ConnectionBuilder connectionBuilder;
  
  /**
   * Class used to allow extending the connection building functionality.
   */
  class ConnectionBuilder {

    /**
     * Creates an HTTP connection to the configured endpoint address. This
     * connection is setup for a POST request, and uses the content type and
     * accept header values in the configuration.
     *
     * @return the connection object
     * @throws IOException on any connection error
     */
    public HttpURLConnection getConnection() throws IOException {
      HttpURLConnection write_connection = (HttpURLConnection) write_endpointUrl.openConnection();
      write_connection.setRequestMethod("POST");
      write_connection.setRequestProperty("Content-Type", "application/json");
      write_connection.setRequestProperty("Accept", "text/plain");
      write_connection.setConnectTimeout(connectTimeout);
      write_connection.setReadTimeout(requestTimeout);
      write_connection.setDoOutput(true);
      write_connection.setDoInput(true);
      write_connection.connect();
      return write_connection;
    }
  }
  
  /**
   * Import configuration parameter from the configuration file 
   */
  //@Override
  public final void configure(final Context context) {
    
    hostname = context.getString("hostname", DEFAULT_HOSTNAME);
    LOG.info("Read InfluxDB hostname from configuration : " + hostname);
    
    port = context.getInteger("port", DEFAULT_PORT);
    LOG.info("Read InfluxDB port from configuration : " + port);
    
    database = context.getString("database", DEFAULT_DATABASE);
    LOG.info("Read InfluxDB database from configuration : " + database);
    
    String write_endpoint = "http://" + hostname + ":" + port + "/write?db=" + database;
    LOG.info("Used Write Endpoint is : " + write_endpoint);
    
    try {
      write_endpointUrl = new URL(write_endpoint);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Endpoint URL invalid", e);
    }
    
    batchSize = context.getInteger("batchSize", DEFAULT_BATCH_SIZE);
    LOG.info("Using batch size : " + batchSize);

    connectTimeout = context.getInteger("connectTimeout", DEFAULT_CONNECT_TIMEOUT);
    if (connectTimeout <= 0) {
      throw new IllegalArgumentException("Connect timeout must be a non-zero and positive");
    }
    LOG.info("Using connect timeout : " + connectTimeout);

    requestTimeout = context.getInteger("requestTimeout", DEFAULT_REQUEST_TIMEOUT);
    if (requestTimeout <= 0) {
      throw new IllegalArgumentException("Request timeout must be a non-zero and positive");
    }
    LOG.info("Using request timeout : " + requestTimeout);

    if (this.sinkCounter == null) {
      this.sinkCounter = new SinkCounter(this.getName());
    }

    connectionBuilder = new ConnectionBuilder();
    
    if ( isActiveInflux(hostname, port) ) {
      LOG.debug("Influx Server is Active");
    } else {
      LOG.debug("Influx Server is NOT Active");
    }
    //createDatabase(hostname, port, database);
  }
  
  /**
   * Create the database name in the InfluxDB instance
   * @param hostname of InfluxDB instance
   * @param port of InfluxDB instance
   * @param database of InfluxDB instance to use
   * @return true  the database has been created
   *         false the database has NOT been created
   */
  public boolean createDatabase(String hostname, int port, String database) {
    String createDBEndpoint = new String("http://" + hostname + ":" + port + "/query");
    createDBEndpoint += "?q=create%20database%20" + database + ";";
    LOG.debug("Create-database endpoint: " + createDBEndpoint);
    try {
      cdbEndpointUrl = new URL(createDBEndpoint);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Endpoint URL invalid", e);
    }
    
    try {
      HttpURLConnection cdbConnection = null;
      cdbConnection = (HttpURLConnection) cdbEndpointUrl.openConnection();
      cdbConnection.setRequestMethod("POST");
      cdbConnection.setRequestProperty("Content-Type", "application/json");
      cdbConnection.setRequestProperty("Accept", "text/plain");
      cdbConnection.setConnectTimeout(connectTimeout);
      cdbConnection.setReadTimeout(requestTimeout);
      cdbConnection.setDoOutput(true);
      cdbConnection.setDoInput(true);
      int httpStatusCode = cdbConnection.getResponseCode();
      if ( httpStatusCode == CREATION_DB_OK) {
        LOG.debug("Creation DB OK");
        return true;
      } else {
        LOG.debug("Creation DB NOT OK");
        return false;
      }
    } catch (IOException e) {
      LOG.debug("Error during the HTTP protocol");
      return false;
    }
  }
  
   /**
    * Check if the InfluxDB instance is active
    * @param hostname of InfluxDB instance
    * @param port of InfluxDB instance
    * @return true  the InfluxDB is active
    *         false the InfluxDB is not active
    */
  public boolean isActiveInflux(String hostname, int port) {
    String pingEndpoint = new String("http://" + hostname + ":" + port + "/ping");
    LOG.debug("Ping endpoint: " + pingEndpoint);
    try {
      pingEndpointUrl = new URL(pingEndpoint);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Endpoint URL invalid", e);
    } 
    try {
      HttpURLConnection pingConnection = (HttpURLConnection) pingEndpointUrl.openConnection();
      pingConnection.setRequestMethod("GET");
      pingConnection.setRequestProperty("Content-Type", "application/json");
      pingConnection.setRequestProperty("Accept", "text/plain");
      pingConnection.setConnectTimeout(connectTimeout);
      pingConnection.setReadTimeout(requestTimeout);
      pingConnection.setDoOutput(true);
      pingConnection.setDoInput(true);
      int statusCode = pingConnection.getResponseCode();
      if ( statusCode == HTTP_STATUS_OK) {
        return true;
      } else {
        return false;
      }
    // disable sink
    } catch (Throwable t) {  
      LOG.error("FATAL: PING Impossible find the database. " + 
          "Restart or reconfigure Flume to continue processing.", t);
      Throwables.propagate(t);
      return false;
    }
  }
  
  /**
   *  Initialization function
   *  The sink counter is started
   */
  @Override
  public final void start() {
    LOG.info("Starting InfluxDBSink");
    sinkCounter.start();
  }

  /**
   * Closing step
   * The sink counter is stopped
   */
  @Override
  public final void stop() {
    LOG.info("Stopping InfluxDBSink");
    sinkCounter.stop();
  }

  //@Override
  public final Status process() throws EventDeliveryException {
    Status status = Status.READY;
    OutputStream outputStream = null;
    ArrayList<String> records = new ArrayList<String>();
    byte[] eventBody = null;
    Channel ch = getChannel();
    Transaction txn = ch.getTransaction();
    txn.begin();
    
    try {
      long i = 0;
      for (; i < batchSize; i++) {
        Event event = ch.take();
        if (event != null) {
          eventBody = event.getBody();
          if (eventBody != null && eventBody.length > 0) {
            records.add( "" + new String( eventBody ));
          }
        } else {
          if (i == 0) {
            status = Status.BACKOFF;
            sinkCounter.incrementBatchEmptyCount();
          } else {
            sinkCounter.incrementBatchUnderflowCount();
          }
          break;
        }
      }
      if (i == batchSize) {
        sinkCounter.incrementBatchCompleteCount();
      }
      sinkCounter.addToEventDrainAttemptCount(i);
      String joinedRecords = Joiner.on("\n").join(records);
      byte[] bytes = joinedRecords.getBytes();
      
      try {
        if ( records.size() == 0 ) {
          txn.commit();
          LOG.debug("No data to send");
          status = Status.READY;
        } else {
          LOG.debug("Sending " + records.size() + " requests");
          HttpURLConnection out_connection = connectionBuilder.getConnection();
          outputStream = out_connection.getOutputStream();
          outputStream.write(bytes);
          outputStream.flush();
          outputStream.close();
    
          int httpStatusCode = out_connection.getResponseCode();
          LOG.debug("Got status code : " + httpStatusCode);
    
          out_connection.getInputStream().close();
          //LOG.debug("Response processed and closed");

          //if (httpStatusCode >= HTTP_MIN_SUCCESS && httpStatusCode <= HTTP_MAX_SUCCESS) {
          if (httpStatusCode == HTTP_STATUS_OK || httpStatusCode == CREATION_DB_OK ) {
            sinkCounter.addToEventDrainSuccessCount(records.size());
            txn.commit();
          } else {
            if (httpStatusCode >= HTTP_MIN_4XX && httpStatusCode <= HTTP_MAX_4XX) {
              LOG.warn("Influx server not understand the request");
            }
            if (httpStatusCode >= HTTP_MIN_5XX && httpStatusCode <= HTTP_MAX_5XX) {
              LOG.warn("Influx system is overloaded or significantly impaired.");
            }
            txn.rollback();
            status = Status.BACKOFF;
          }
        }
      } catch (IOException e) {
        txn.rollback();
        status = Status.BACKOFF;
        LOG.error("Error opening connection, or request timed out", e);
      }
    } catch (Throwable t) {
      txn.rollback();
      status = Status.BACKOFF;

      LOG.error("Error retrieving data from channel", t);
      // re-throw all Errors
      if (t instanceof Error) {
        throw (Error) t;
      }
    } finally {
      txn.close();

      if (outputStream != null) {
        try {
          outputStream.close();
        } catch (IOException e) {
          // ignore errors
        }
      }
    }
    return status;
  }

  /**
   * Update the connection builder.
   *
   * @param builder  the new value
   */
  final void setConnectionBuilder(final ConnectionBuilder builder) {
    this.connectionBuilder = builder;
  }

  /**
   * Update the sinkCounter.
   *
   * @param newSinkCounter  the new value
   */
  final void setSinkCounter(final SinkCounter newSinkCounter) {
    this.sinkCounter = newSinkCounter;
  }
}