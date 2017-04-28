/*
 * Based on:  
 * - SyslogUDPSource (Apache Flume Syslog UDP Source)
 * - https://github.com/ottomata/flume-ng/tree/udp-source
 * Apache License 2.0. See:
 * - https://github.com/apache/flume/blob/trunk/NOTICE
 * - https://github.com/apache/flume/blob/trunk/LICENSE
 */

package ch.cern.alice.o2.flume;

/**
 * Apache flume UDP/JSON Source configuration
 * @author Adam Wegrzynek
 */
public final class UDPSourceConfigurationConstants {

  /**
   * UDP port configuration name
   */
  public static final String  CONFIG_PORT       = "port";
  
  /**
   * UDP hostname configuration name
   */
  public static final String  CONFIG_HOST       = "host";

  /**
   * UDP hostname value
   */
  public static final String  DEFAULT_HOST      = "0.0.0.0";

  /**
   * Event delimiter configuration name
   */
  public static final String  CONFIG_DELIMITER  = "delimiter";

  /**
   * Event delimier character
   */
  public static final String  DEFAULT_DELIMITER = "\n";
  
  /**
   * Maximum size of packet name
   */
  public static final String  CONFIG_MAXSIZE    = "maxsize";

  /**
   * Maximum size of packet value
   * 64k is max allowable in RFC 5426
   */
  public static final Integer DEFAULT_MAXSIZE   = 1 << 16;

  private UDPSourceConfigurationConstants() {
    // Disable explicit creation of objects.
  }
}
