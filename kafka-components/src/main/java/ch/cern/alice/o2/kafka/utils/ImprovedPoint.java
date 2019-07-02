package ch.cern.alice.o2.kafka.utils;

import java.util.concurrent.TimeUnit;

import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ImprovedPoint{
	
	private static Point point;
	private static Logger logger = LoggerFactory.getLogger(ImprovedPoint.class);

	public ImprovedPoint(byte [] byteLineProtocol) throws Exception {
		point = importFromLineProtocol(new String(byteLineProtocol).trim());
	}
	
	public ImprovedPoint(String stringLineProtocol) throws Exception {
		point = importFromLineProtocol(stringLineProtocol.trim());
	}
	
	public Point getPoint() {
		return point;
	}
	
	public Point importFromLineProtocol(String lineprot) throws Exception {
		String [] lineprot_vett = lineprot.split(" ");
		if( lineprot_vett.length < 2 || lineprot_vett.length > 3) {
			logger.error("Influxdb line protocol contains a odd number of fields: "+lineprot_vett.length);
			logger.error(lineprot);
			throw new Exception("Illegal Line protocol format - number of fields: "+lineprot_vett.length);
		} else {
			String [] measTags_vett = lineprot_vett[0].split(",");
			Builder tempPoint = Point.measurement(measTags_vett[0]);
			for(int i = 1; i < measTags_vett.length; i++) {
				String [] keyValueTag = measTags_vett[i].split("=");
				if( keyValueTag.length == 2) {
					tempPoint.tag(keyValueTag[0],keyValueTag[1]);
				} else {
					logger.error("'"+lineprot_vett[0]+"' not well formatted");
					throw new Exception("Illegal Line protocol format - tag section");
				}
			}
			String [] fields_vett = lineprot_vett[1].split(",");
			for( String fields_item: fields_vett) {
				String [] keyvalue = fields_item.split("=");
				if( keyvalue.length == 2) {
					String fieldName = keyvalue[0];
					String fieldValue = keyvalue[1];
					boolean isConverted = false;
					if( fieldValue.charAt(fieldValue.length()-1) == 'i') {
						try {
							tempPoint.addField(fieldName, Long.parseLong(fieldValue.substring(0, fieldValue.length()-1)));
							isConverted = true;
						} catch(Exception e) {
							isConverted = false;
						}
					} 	
					if(! isConverted) {
						try{
							tempPoint.addField(fieldName, Double.parseDouble(fieldValue));
						} catch(Exception e1) {
							try{
								tempPoint.addField(fieldName, Boolean.parseBoolean(fieldValue));
							} catch(Exception e2) {
								tempPoint.addField(fieldName, fieldValue.replace("\"", ""));
							}
						}
					}
				} else {
					logger.error("'"+lineprot_vett[1]+"' not well formatted");
					throw new Exception("Illegal Line protocol format - field section");
				}
			}
			if( lineprot_vett.length == 3) {
				tempPoint.time(Long.parseLong(lineprot_vett[2]), TimeUnit.NANOSECONDS);
			}
			return tempPoint.build();
		}
	}
}