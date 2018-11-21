package message_etl_kafka;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Reducer;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import messageETL.DataForm;
import messageETL.LogCounter;
import messageETL.logParser;

public class Main {
	private static final Logger logger = LogManager.getLogger(Main.class.getName());
	private static final Common commonUtil = new Common();
	private static String current_date = "20180720";
	private static String current_mashost = "MAS01";
	private static long msgoffset = 0; 
	
	public static void main(String[] args) {
		
		String a = ",,,,굿모닝의료\n[";
		System.out.println(a.length());
		try {
			System.out.println(a.getBytes("EUC-KR").length);
		} catch (UnsupportedEncodingException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		if(true)
			return;
		logger.debug("config setting");
		Properties props = commonUtil.readProperty();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, props.getOrDefault("APPLICATION_ID_CONFIG", "ETL_Processor_0"));
		//props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "swinno04.cs9046cloud.internal:6667");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("BOOTSTRAP_SERVERS_CONFIG", "localhost:9092"));
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, props.getProperty("NUM_STREAM_THREAD", "1"));
		//logger.debug("StreamsConfig complete- "+ props.toString());
		
		logger.info("Build Stream");
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> inputStream = builder.stream(props.getProperty("KAFKA_INPUT_TOPIC", "my"));
		//KStream<String, ArrayList<String>> lineparsed = inputStream.flatMapValues(value -> Arrays.asList(value.split("\\W+")));
		
	/*	inputStream.foreach(new ForeachAction<String, String>() {
		    public void apply(String key, String value) {
		        System.out.println(key + ": " + value.length());
		    }
		 });
		*/
		KStream<String, String> parsedLog = inputStream
				.mapValues(value -> parseLogBlock(value))		//log parsing
				.filter((key, value) -> value != null);		//filtering null value(ejected logs)
		
		parsedLog.to(props.getProperty("KAFKA_OUTPUT_TOPIC", "myOutput"));
		//aggregatedStream.toStream().to(props.getProperty("KAFKA_OUTPUT_TOPIC", "myOutput"));
//		parsedLog.to("nonagg");
	//	parsedLog.to(props.getProperty("KAFKA_OUTPUT_TOPIC", "myOutput"));
		
		
		final KafkaStreams streams = new KafkaStreams(builder.build(), props);
		try {
			logger.info("Starts stream");
			streams.start();
		}catch(Exception e) {
			logger.error("Start stream failed", e.getMessage());
		}
		
		Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

	}
	
	
	/**
	 * @breif kafka에서 읽은 로그 문자열 처리. 중요도가 낮은 로그는 nul	l 로 리턴한다.
	 * @param originalTxt
	 * @return
	 */
	public static String parseLog(String originalTxt) {
		//logger.debug(originalTxt);
		/*
		 * try { Thread.sleep(1000); } catch (InterruptedException e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); }
		 */

		// String msg = getMessageString(originalTxt);
		// if(msg.length()==0)
		// return "";
		DataForm data = generateDataForm(originalTxt);
		if (data != null) {
			String result = "";
			data.setTime(current_date + "-" + data.getTime());
			JSONObject jsonObj;
			try {
				jsonObj = maketoJson(data);
				result = jsonObj.toString();
			} catch (Exception je) {
				logger.error("parsing Error - " + originalTxt);
				logger.error(je.getMessage());
			}
			//logger.debug("[parseLog] result - " + result);
			return result;
		} else {
		//	logger.info("result of \'generateDataForm\' is null. return null.");
			return null;
		}
	};
	
	
		
	@SuppressWarnings("unchecked")
	static 	JSONObject maketoJson(DataForm df) throws Exception
	{
		JSONObject obj = new JSONObject ();
		obj.put("logid", df.getLogID());
		obj.put("jobid", df.getJobID());
		obj.put("c_msg_id", df.getCustomMessageID());
		obj.put("type", df.getType());
		obj.put("time", df.getTime());
		obj.put("result", df.getResult());
		obj.put("host", df.getHost());
		obj.put("parentid", df.getParentID());
		obj.put("spid", df.getSpid());
		obj.put("recivenum", df.getRecivenumber());
		obj.put("callbacknum", df.getCallbacknumber());
		
		obj.put("mas_host", df.getMasHost());
		return obj;
		
	}
	
	/**
	 * @brief 로그 데이터에서 DataForm 객체 생성
	 * @param log
	 * @return
	 */
	public static DataForm generateDataForm(String log) {
		DataForm result = new DataForm();
		logParser parser = new logParser();
		if(log.contains("<?xml")) {
			String[] strArr = log.split("\n");
			result = parser.Parsebyxml(strArr, strArr.length);
		}
		else {
			result  =parser.ParsebyLine(log);
		}
		return result;
	}
	
	/**
	 * @brief logstash가 생성한 kafka message에서 로그 부분만 발췌
	 * @param msg
	 * @return
	 */
	public static String getMessageString(String msg) {
		String result="";
		//get message string
				JSONParser jsonParser = new JSONParser();
				JSONObject jsonObj;
				try {
					jsonObj = (JSONObject) jsonParser.parse(msg);
					result = (String) jsonObj.get("message");
					//logger.debug("parsed log: "+ result);
				} catch (ParseException e) {
					logger.error("Parsing error - " + e.getMessage());
					e.printStackTrace();
				}
		return result;
	}
	
	/**
	 * @brief input for 100~10000 lines just parse line with \n[
	 * @param msg
	 * @return
	 */
	
	static String getFirstToken(String start, String end, String str)
	{
		int startidx = str.indexOf(start);
		int endidx = -1;
		if(startidx == -1) return "";
		int j =0;
		for(int i = startidx + start.length() ;i<str.length();i++) {
			for(j=0;j<end.length();j++)
			{
				if(str.charAt(i) == end.charAt(j))
				{
					endidx = i;
					break;
				}
			}
			if(j!= end.length())
				break;
		}
		if(end.equals("\n"))
			endidx = str.length();

		if(startidx == -1 || endidx == -1 || startidx>endidx)
			return "";
		
		if(str.substring(startidx+start.length(),endidx).equals("true") || str.substring(startidx+start.length(), endidx).equals("false") )
			return "";
		return str.substring(startidx+start.length(), endidx);
	}
	
	/**
	 * @brief input for 100~10000 lines just parse line with \n
	 * @param msg
	 * @return
	 */
	
	public static String [] parseHedaerInfo(String str)
	{
		String callback = str;
		String header;
		String mashost = "";
	//	System.out.println("header in >> " + str);
		if(str.contains("<</HEADER>>\n") )
		{
			header = str.substring(0,str.indexOf("<</HEADER>>\n") + 11);
			current_date = getFirstToken("date:\"","\"",header);
			mashost = getFirstToken("mas_host:\"","\"",header);
			current_mashost = mashost;
			System.out.println("Currentdate :" + current_date  + "  MASHost :" + current_mashost);
			callback = str.substring(str.indexOf("<</HEADER>>\n") + 12);
		}
		else
		{
			System.out.println("[WARN] Header is not included");
			mashost = "";
			current_mashost = "";
		}
		String cback[] = new String[2];
		cback[0] = callback;
		cback[1] = mashost;
		return cback;		
	}
	
	/**
	 * @brief input for 100~10000 lines just parse line with \n
	 * @param msg
	 * @return
	 */
	
	public static ArrayList<String> splitLines(String str)
	{
		msgoffset++;
		if(msgoffset < 0 ) msgoffset = 0;
		System.out.println(msgoffset);
		
		String [] header = new String[2];
		String mashost;
		header = parseHedaerInfo(str);
		str = header[0];
		mashost = header[1];
		ArrayList<String> arr = new ArrayList<String>(); 
		String stemp = str;
		while(true)
		{
			//spliter \n[ \0\n[ \n<?xml 
			int pp = stemp.indexOf("\n[");
			if(pp == -1)
			{
				arr.add(stemp);
				break;
			}
			arr.add( stemp.substring(0,pp) );
			stemp = stemp.substring(pp+1);	
			
		}
		return arr;
		
	}
	/**
	 * @breif kafka에서 읽은 로그 문자열 처리. 중요도가 낮은 로그는 nul	l 로 리턴k한다. -block 단위처리한다.
	 * @param originalTxt
	 * @return
	 */
	public static String parseLogBlock(String originalTxt) {
		//System.out.println("-----------------------------------length :" + originalTxt.length() + " ---------------------------------------");
		//	System.out.println(originalTxt);
		
		//logger.debug(originalTxt);
		/*
		 * try { Thread.sleep(1000); } catch (InterruptedException e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); }
		 */

		// String msg = getMessageString(originalTxt);
		// if(msg.length()==0)
		// return "";
		String str = originalTxt;
		
		String [] header = new String[2];
		String mashost;
		header = parseHedaerInfo(str);
		str = header[0];
		mashost = header[1];	
		
		String[] strlist = new String[100000];
		String [] bufferedstr = new String [100000];
		
		strlist = str.split("\n");
		ArrayList<DataForm> datalist = new ArrayList<DataForm>();;
		logParser parser = new logParser();
		boolean isxml = false;
		int xmlcount = 0;		
		int scnt=-1;
		int logcounter=0;
		
	//	System.out.println(strlist.length);
		while(++scnt<strlist.length){
			String s = strlist[scnt];
			
		//	System.out.println(s + logcounter);
			if(s.length() < 3) continue;
			if(s.contains("<?xml") )
			{
				if(isxml == true)
				{
					// </RCP> <?xml version="1.0" encoding="UTF-8"?> case
					if(s.contains("</RCP>") || s.contains("</MAS>") || s.contains("</MAS_REPORT>"))
					{
						//xmlcount++;
						DataForm data = null;
						if(datalist.size()>=1)
							data = datalist.get(datalist.size()-1);
						DataForm data2 = parser.Parsebyxml(bufferedstr,xmlcount);
						
						if(isCombineable(data,data2))
						{
							datalist.remove(datalist.size()-1);
							DataForm combinedata = CombineData(data, data2);
							
							datalist.add(combinedata);
							LogCounter.count--;
							LogCounter.count_normal--;
							logcounter--;
						}
						else
						{
							if(data2!= null)
							{
								datalist.add(data2);
								logcounter++;
							}
						}						
						isxml= false;
						xmlcount = 0;
						s = s.replace("</RCP>", "");
						s = s.replace("</MAS>", "");
						s = s.replace("</RCP>", "");
					}
					else
					{
						System.out.println("what else line? : " + s);
					}
				}
				isxml = true;
				xmlcount = 0;
			}
			if( isxml ) 
			{
				if(xmlcount >= 100000)
				{
				//	System.out.println(s);
					isxml = false; 
					xmlcount = 0;
					continue;
				}
				bufferedstr[xmlcount++] = s;
				if(s.length()<=1) continue;
				if(s.contains("</RCP>") || s.contains("</MAS><xml") || s.contains("</MAS_REPORT>") || s.contains("</MAS>"))
				{
					DataForm data = null;
					if(datalist.size()>=1)
						data = datalist.get(datalist.size()-1);
					DataForm data2 = parser.Parsebyxml(bufferedstr,xmlcount);
					if(isCombineable(data,data2))
					{
						datalist.remove(datalist.size()-1);
						DataForm combinedata = CombineData(data, data2);
						datalist.add(combinedata);
						LogCounter.count--;
						LogCounter.count_normal--;
						logcounter--;
					}
					else
					{
						if(data2!= null)
						{
							datalist.add(data2);
							logcounter++;
						}
					}
							
					isxml= false;
					xmlcount = 0;
				}
				else if(s.charAt(0) == '[')
				{
					isxml = false;
					
					System.out.println("Xml read unexcept parse Error : " + s);
					if(xmlcount !=0)
						System.out.println("Xml : " + bufferedstr[xmlcount-1]);
					else
						System.out.println("Xml : " + bufferedstr[xmlcount]);
					//else if()
					DataForm data = parser.ParsebyLine(s);
					if(data!= null)
					{
						datalist.add(data);
						logcounter++;
					}
					xmlcount = 0;
					continue;
				}
				else
				{
				}
				//if(s.isEmpty() || s.charAt(0) == '[')
					//isxml = false;
				 
			}	 
			else
			{
				DataForm data = parser.ParsebyLine(s);
				if(data!= null)
				{
					datalist.add(data);
					logcounter++;
				}
			}				
		}
		String result = "";
		for(int i=0;i<datalist.size();i++)
		{
		
			DataForm df = datalist.get(i);
			df.setTime(current_date + "-" + df.getTime());
			df.setMasHost(mashost);
			JSONObject jsonobj;
			try {
				jsonobj = maketoJson(df);
				if(result.length() == 0)
					result = jsonobj.toString();
				else
					result = result + "\n" + jsonobj.toString();
				if(result.length() > originalTxt.length()*2)
				{
					System.out.println("------------DEBUG NEEDED----------  ETL result is longer than orginal*2");
					break;
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	//	System.out.println("datalist.size : " + datalist.size() + " result length : " + result.length() );
		return result;
	}
	static boolean isCombineable(DataForm df1, DataForm df2)
	{
		if(df1== null)
			return false;
		//case send && xml
		if( df1.getType().compareTo("Send :") != 0  && df1.getType().compareTo("XML Received") != 0  )
			return false;
		if(! df2.getType().contains("xml" ))
			return false;
		return true;
	}
	static DataForm CombineData(DataForm df1, DataForm df2)
	{
		
		DataForm df = new DataForm();
		df.setCustomMessageID(df2.getCustomMessageID());
		df.setLogID(df1.getLogID());
		df.setHost(df1.getHost());
		df.setJobID(df2.getJobID());
		df.setParentID(df1.getParentID());
		df.setResult(df2.getResult());
		df.setTime(df1.getTime());
		df.setType(df2.getType());
		df.setRecivenumber(df2.getRecivenumber());
		df.setCallbacknumber(df2.getCallbacknumber());
		return df;
	}

}
