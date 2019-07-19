package ch.cern.alice.o2.kafka.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.lang.Object;
import java.util.concurrent.TimeUnit;
import java.math.BigDecimal;
import org.javatuples.Triplet; 
import java.text.NumberFormat;
import java.util.Locale;

public class LineProtocol {
	private static Logger logger = LoggerFactory.getLogger(LineProtocol.class); 
	private static String measurement;
	private static Map<String,String> tags = new HashMap<String,String>();
	private static Map<String,Object> fields = new HashMap<String,Object>();
	private static Long time;
	private static TimeUnit precision = TimeUnit.NANOSECONDS;
	private static final int MAX_FRACTION_DIGITS = 340;
	
	public LineProtocol() { }
	
	public LineProtocol(String meas, Map<String,String> new_tags, Map<String,Object> new_fields, Long new_ts){
		measurement = meas;
		for(Map.Entry<String,String> tag: new_tags.entrySet()) {
			tags.put(tag.getKey(),tag.getValue());
		}
		for(Map.Entry<String,Object> field: new_fields.entrySet()) {
			fields.put(field.getKey(),field.getValue());
		}
		time = new_ts;
	}
		
	public List<Triplet<String, Double, String>> getTriplets(String func){
		List<Triplet<String, Double, String>> llp = new ArrayList<Triplet<String, Double, String>>();
		for(Map.Entry<String,Object> field: fields.entrySet()) {
			String strKey = measurement;
			//improve code here!!!!!
			for(Map.Entry<String,String> tag: tags.entrySet()) {
				strKey += "," + tag.getKey() + "=" + tag.getValue();
			}
			strKey += "|"+field.getKey();
			Object value = field.getValue();
			Double double_value ;
			if( value instanceof Long) {
				Long l = new Long((long) value);
				double_value = l.doubleValue();
				Triplet<String, Double, String> temp = new Triplet<String, Double, String>(strKey,double_value,func);
				llp.add( temp );
			} else {
				try {
					double_value = (Double) value;
					Triplet<String, Double, String> temp = new Triplet<String, Double, String>(strKey,double_value,func);
					llp.add( temp );
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return llp;
	}
	
	public void addTag(String key, String value) {
		tags.put(key, value);
	}
	
	public void addValue(String key, Object value) {
		fields.put(key, value);
	}
	
	public void setTimestamp(Long new_time, TimeUnit new_precision) {
		time = new_time;
		precision = new_precision;
	}
	
	public void printFields() {
		System.out.println(fields);
	}
	
	public String getMeasurement() {
		return measurement;
	}
	
	public LineProtocol dropTagKeys(String[] tagToDrop_vett) {
		for(String tagToDrop: tagToDrop_vett) {
			dropTagKey(tagToDrop);
		}
		return this;
	}
	
	public LineProtocol dropTagKey(String tagToDrop) {
		if(tags.containsKey(tagToDrop)) {
			tags.remove(tagToDrop);
		}
		return this;
	}
	
	public LineProtocol dropNotNumberFields() {
		Set<String> keys_to_remove = new HashSet<String>();
		for( Map.Entry<String,Object> entry: fields.entrySet()) {
			Object value = entry.getValue();
			if( (value instanceof String) || (value instanceof Boolean)) {
				keys_to_remove.add(entry.getKey());
			}
		}
		for(String key_to_remove: keys_to_remove) {
			fields.remove(key_to_remove);
		}
		return this;
	}
	
	public List<LineProtocol> splitFields(){
		List<LineProtocol> metrics = new ArrayList<LineProtocol>();
		for( Map.Entry<String,Object> field: fields.entrySet()) {
			Map<String,Object> new_fields = new HashMap<String,Object>();
			new_fields.put(field.getKey(), field.getValue());
			metrics.add(new LineProtocol(measurement,tags,new_fields,time));
		}
		return metrics;
	}
	
	public LineProtocol fromLineProtocol(String lineprot) throws Exception {
		measurement = "";
		tags = new HashMap<String,String>();
		fields = new HashMap<String,Object>();
		time = 0L;
				
		String [] lineprot_vett = lineprot.split(" ");
		if( lineprot_vett.length < 2 ) {
			logger.error("Influxdb line protocol contains a odd number of fields: "+lineprot_vett.length);
			logger.error(lineprot);
		} else {
			String [] measTags_vett = lineprot_vett[0].split(",");
			measurement = measTags_vett[0];
			for(int i = 1; i < measTags_vett.length; i++) {
				String [] keyValueTag = measTags_vett[i].split("=");
				if( keyValueTag.length == 2) {
					tags.put(keyValueTag[0],keyValueTag[1]);
				} else {
					logger.error("'"+lineprot_vett[0]+"' not well formatted");
				}
			}
			
			int field_index_begin = 1;
			int field_index_end = lineprot_vett.length;
			try {
				time = Long.parseLong(lineprot_vett[lineprot_vett.length-1]);
				field_index_end = lineprot_vett.length - 1;
			} catch (Exception e){
				time = 0L;
			}
			
			// If isThereTimestamp is 'true' then the last element of lineprot_vett will not be used to build the line protocol field section
			String field_vett[] = String.join(" ", Arrays.copyOfRange(lineprot_vett, field_index_begin, field_index_end)).split(",");
			for(String field: field_vett) {
				String [] keyvalue = field.split("=");
				if( keyvalue.length == 2) {
					String fieldName = keyvalue[0];
					String fieldValue = keyvalue[1];
					boolean isConverted = false;
					if( fieldValue.charAt(fieldValue.length()-1) == 'i') {
						try {
							fields.put(fieldName, Long.parseLong(fieldValue.substring(0, fieldValue.length()-1)));
							isConverted = true;
						} catch(Exception e) {
							isConverted = false;
						}
					} 	
					if(! isConverted) {
						try{
							fields.put(fieldName, Double.parseDouble(fieldValue));
						} catch(Exception e1) {
							if(fieldValue.toLowerCase().equals("true")) {
								fields.put(fieldName, true);
							} else {
								if(fieldValue.toLowerCase().equals("false")) {
									fields.put(fieldName, false);
								} else {
									fields.put(fieldName, fieldValue.replace("\"", ""));
								}
							}
						}
					}
				} else {
					logger.error("'"+lineprot_vett[1]+"' not well formatted");
					//throw new Exception("Illegal Line protocol format - field section");
				}
			}
		}
		return this;
	}
	
	public String toLineProtocol(TimeUnit precision ) {

		// setLength(0) is used for reusing cached StringBuilder instance per thread
	    // it reduces GC activity and performs better then new StringBuilder()
	    StringBuilder sb = new StringBuilder();
	    sb.setLength(0);
	    escapeKey(sb, measurement);
	    concatenatedTags(sb);
	    concatenatedFields(sb);
	    formatedTime(sb, precision);

	    return sb.toString();
	}
	
	private void concatenatedTags(final StringBuilder sb) {
	    for (Entry<String, String> tag : LineProtocol.tags.entrySet()) {
	      sb.append(',');
	      escapeKey(sb, tag.getKey());
	      sb.append('=');
	      escapeKey(sb, tag.getValue());
	    }
	    sb.append(' ');
	  }
	
	private static final ThreadLocal<NumberFormat> NUMBER_FORMATTER =
	          ThreadLocal.withInitial(() -> {
	            NumberFormat numberFormat = NumberFormat.getInstance(Locale.ENGLISH);
	            numberFormat.setMaximumFractionDigits(MAX_FRACTION_DIGITS);
	            numberFormat.setGroupingUsed(false);
	            numberFormat.setMinimumFractionDigits(1);
	            return numberFormat;
	});
	

	  private void concatenatedFields(final StringBuilder sb) {
	    for (Entry<String, Object> field : LineProtocol.fields.entrySet()) {
	      Object value = field.getValue();
	      if (value == null) {
	        continue;
	      }
	      escapeKey(sb, field.getKey());
	      sb.append('=');
	      if (value instanceof Number) {
	        if (value instanceof Double || value instanceof Float || value instanceof BigDecimal) {
	          sb.append(NUMBER_FORMATTER.get().format(value));
	        } else {
	          sb.append(value).append('i');
	        }
	      } else if (value instanceof String) {
	        String stringValue = (String) value;
	        sb.append('"');
	        escapeField(sb, stringValue);
	        sb.append('"');
	      } else {
	        sb.append(value);
	      }

	      sb.append(',');
	    }

	    // efficiently chop off the trailing comma
	    int lengthMinusOne = sb.length() - 1;
	    if (sb.charAt(lengthMinusOne) == ',') {
	      sb.setLength(lengthMinusOne);
	    }
	  }

	  static void escapeKey(final StringBuilder sb, final String key) {
	    for (int i = 0; i < key.length(); i++) {
	      switch (key.charAt(i)) {
	        case ' ':
	        case ',':
	        case '=':
	          sb.append('\\');
	        default:
	          sb.append(key.charAt(i));
	      }
	    }
	  }

	  static void escapeField(final StringBuilder sb, final String field) {
	    for (int i = 0; i < field.length(); i++) {
	      switch (field.charAt(i)) {
	        case '\\':
	        case '\"':
	          sb.append('\\');
	        default:
	          sb.append(field.charAt(i));
	      }
	    }
	  }

	  private void formatedTime(final StringBuilder sb, final TimeUnit precision) {
	    if (LineProtocol.time == null) {
	      return;
	    }
	    if (precision == null) {
	      sb.append(" ").append(TimeUnit.NANOSECONDS.convert(LineProtocol.time, LineProtocol.precision));
	      return;
	    }
	    sb.append(" ").append(precision.convert(LineProtocol.time, LineProtocol.precision));
	  }

	

}