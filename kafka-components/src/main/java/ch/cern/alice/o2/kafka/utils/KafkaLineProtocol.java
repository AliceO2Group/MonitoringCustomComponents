package ch.cern.alice.o2.kafka.utils;

/**
 * 
 */


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

public class KafkaLineProtocol {
	private static Logger logger = LoggerFactory.getLogger(KafkaLineProtocol.class); 
	
	private String lp = null;
	private String key = null;
	private String value = null;
	private String [] vettMeasFieldname = null;
	private String [] vettTagFieldvalueTimestamp = null;

	/*private static String measurement;
	private static Map<String,String> tags = new HashMap<String,String>();
	private static Map<String,Object> fields = new HashMap<String,Object>();
	private static Long time;
	private static TimeUnit precision = TimeUnit.NANOSECONDS;
	private static final int MAX_FRACTION_DIGITS = 340;
	*/
	
	public KafkaLineProtocol() { }
	
	public KafkaLineProtocol(String lp){
		this.lp = lp;
	}

	public KafkaLineProtocol(String key, String value){
		this.key = key;
		this.value = value;
	}

	public String getKey(){
		//if( this.key != null)
		return this.key.trim();
	} 

	public String getValue(){
		//if( this.value != null)
		return this.value.trim();
	}

	public List<KafkaLineProtocol> getKVsFromLineProtocol(){
		List<KafkaLineProtocol> lps = new ArrayList<KafkaLineProtocol>();
		String [] lineprotVett = this.lp.split(" ");
		if( lineprotVett.length < 2 ) {
			logger.error("Influxdb line protocol contains an odd number of fields: "+lineprotVett.length);
			logger.error(lp);
		} else {
			String measTags  = lineprotVett[0];
			String [] vettMeasTags = measTags.split(",");
			String meas = vettMeasTags[0];
			String tags = String.join(",", Arrays.copyOfRange(vettMeasTags, 1, vettMeasTags.length));
			int fieldIndexEnd = 1;
			String time = "";	
			
			// Check whether the last field is a timestamp. 
			// Additional conditions can be used (e.g. a day ago timestamp < t < now)
			try{
				Long.parseLong(lineprotVett[lineprotVett.length-1]);
				time = lineprotVett[lineprotVett.length-1];
				fieldIndexEnd = lineprotVett.length - 1 ;
			} catch (Exception e) {
				fieldIndexEnd = lineprotVett.length;
			}
			String[] fieldsVett = String.join(" ", Arrays.copyOfRange(lineprotVett, 1, fieldIndexEnd)).split(",");
			for(String field: fieldsVett) {
				String [] fieldVett = field.split("=");
				if(fieldVett.length != 2){
					logger.error("importProcessor - 'field' field not well written: "+lp);
				}
				String fieldName = fieldVett[0];
				String fieldValue = fieldVett[1];
				String newKey = meas + "#" + fieldName;
				String newValue = tags + "#" + fieldValue + "#" + time;
				lps.add(new KafkaLineProtocol(newKey.trim(), newValue.trim()));
			}
		}
		return lps;
	}
		
	private KafkaLineProtocol computeVettMeasFieldname() throws Exception {
		if( this.vettMeasFieldname == null ){
			String [] temp = this.key.split("#");
			if(temp.length != 2){
				throw new Exception("Bad key in LP: "+this.key+". Length: "+temp.length);
			} 
			this.vettMeasFieldname = temp;
		}
		return this;
	}

	private KafkaLineProtocol computeVettTagFieldvalueTimestamp() throws Exception{
		if( this.vettTagFieldvalueTimestamp == null ){
			String [] temp = this.value.split("#");
			if(temp.length < 2 || temp.length > 3){
				throw new Exception("Bad value in LP: "+this.value+". Length: "+temp.length);
			} 
			this.vettTagFieldvalueTimestamp = temp;
		}
		return this;
	}

	public String getMeasTagFieldKey(){
		try{
			this.computeVettMeasFieldname().computeVettTagFieldvalueTimestamp();
			String meas = this.vettMeasFieldname[0];
			String fieldName = this.vettMeasFieldname[1];
			String tags = this.vettTagFieldvalueTimestamp[0].trim();
			if( tags == null || tags.equals("")){
				return meas+"#"+fieldName;
			} else {
				return meas+","+tags+"#"+fieldName;
			} 
		} catch (Exception e) {
			logger.warn(e.getMessage());
			return null;
		}
	}

	public String getFieldValue(){
		try{
			this.computeVettTagFieldvalueTimestamp();
			return this.vettTagFieldvalueTimestamp[1];
		} catch (Exception e) {
			logger.warn(e.getMessage());
			return null;
		}
	}

	public String getMeasurement(){
		try{
			this.computeVettMeasFieldname();
			return this.vettMeasFieldname[0];
		} catch (Exception e) {
			logger.warn(e.getMessage());
			return null;
		}
	}

	public String getFieldName(){
		try{
			this.computeVettMeasFieldname();
			return this.vettMeasFieldname[1];
		} catch (Exception e) {
			logger.warn(e.getMessage());
			return null;
		}
	}

	public String getTimestamp(){
		try{
			this.computeVettTagFieldvalueTimestamp();
			if( this.vettTagFieldvalueTimestamp.length > 2){
				return " " + this.vettTagFieldvalueTimestamp[2];
			} else {
				return "";
			}
		} catch (Exception e) {
			logger.warn(e.getMessage());
			return null;
		}
	}

	public String getLineProtocol(){
		try{
			this.computeVettMeasFieldname().computeVettTagFieldvalueTimestamp();
			String meas = this.vettMeasFieldname[0];
			String fieldName = this.vettMeasFieldname[1];
			String tags = this.vettTagFieldvalueTimestamp[0];
			String fieldValue = this.vettTagFieldvalueTimestamp[1];
			String timestamp = this.getTimestamp();
			return meas+","+tags+" "+fieldName+"="+fieldValue+timestamp;
		} catch (Exception e) {
			logger.warn(e.getMessage());
			return null;
		}
	}

	public KafkaLineProtocol removeTags(Set<String> tagsToRemove){
		try{
			this.computeVettTagFieldvalueTimestamp();
			List<String> listNewTags = new ArrayList<String>(); 
			String tags = this.vettTagFieldvalueTimestamp[0];
			for( String tag: tags.split(",")){
				String tagName = tag.split("=")[0];
				if(! tagsToRemove.contains(tagName)){
					listNewTags.add(tag);
				}
			}
			String newTags =  String.join(",",listNewTags);
			String fieldValue = this.vettTagFieldvalueTimestamp[1];
			String timestamp = this.getTimestamp();
			this.value = newTags+"#"+fieldValue+"#"+timestamp;
			this.vettTagFieldvalueTimestamp = null;
			return this;
		} catch (Exception e) {
			logger.warn(e.getMessage());
			return null;
		}
	}

	public Double getDoublefieldValue(){
		String fieldValue = this.getFieldValue();
		try{
			return Double.parseDouble(fieldValue);
		} catch (Exception e1) {
			try{
				Long l = Long.parseLong(fieldValue.substring(0, fieldValue.length()-1));
				return l.doubleValue();
			} catch (Exception e2) {
				logger.warn("The fieldValue is not convertible to double: "+fieldValue);
				return null;
			}
		}
	}
}