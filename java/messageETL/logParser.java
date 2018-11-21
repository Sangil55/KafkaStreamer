package messageETL;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class logParser
{
	List <String> apilist;
	boolean visit[] = new boolean[1000];
	List <String> apilist_eject;
	public logParser()
	{
		apilist = new ArrayList<String>();
		apilist_eject = new ArrayList<String>();
		
		apilist.add("QueryInsertMessage");
		apilist.add("RequestSearchNPDB");
		apilist.add("OnRequestSendMessage2");
		apilist.add("QuerySelectMessage");
		apilist.add("QuerySelectinnerNPDB");
		apilist.add("getCIDInfo");
		apilist.add("IsSpam");
		apilist.add("CheckSameMessage");
		apilist.add("SendJob");
		apilist.add("RetrySendMessageMRS");
		apilist.add("SendReport");
		apilist.add("QueryUpdateMessageReportkACK");
		apilist.add("SendUnreachReport");
		apilist.add("sendnumber");
		apilist.add("OnClose");
		apilist.add("OnRequestRegist");
		
		apilist.add("Logout");
		apilist.add("UnregistLoginPeer");
		apilist.add("StoreUnreachReport");
		apilist.add("CheckEndUserCert");
		apilist.add("ProcessKickOut");
		apilist.add("OnArriveMessage");
		apilist.add("OnIoComplete");
		apilist.add("QuerySelectEUSER");
		apilist.add("OnRequestRegist");
		apilist.add("QueryUpdateFailSend");
		apilist.add("_SendMASFailReport");
		apilist.add("UnregistLoginPeer");
		apilist.add("UNREGIST");
		apilist.add("StoreUnreachReport");
		apilist.add("RemoveAll");
		apilist.add("GetCIDInfo");
		apilist.add("_SendUnreachReport");
		apilist.add("ProcessUnReachMessage");
		apilist.add("RetrySendMessageMRS - Check");
		apilist.add("Success Logout");
		apilist.add("Failed GetVCID");
		apilist.add("Query SCP::Login");
		apilist.add("Success SCP::Login");
		apilist.add("SendReponseStorageInfo");
		apilist.add("RetrySendMessageMRS - ReSend");
		apilist.add("UpdateNotUpdateReport");
		apilist.add("Failed Logout");
		apilist.add("Failed Login");
		apilist.add("OnPerfResetTimer");
		apilist.add("QueryUpdateMessageReportAck");
		apilist.add("QueryDeleteMessageExt");
		apilist.add("QueryDeleteMessageExt");
		
		apilist.add("Send :");
		apilist.add("XML Received");
		apilist.add("OnIoComplete");
		
		
		// �߰�
		apilist.add("Accept");
		apilist.add("Socket Closed :");
		apilist.add("SendM_UNREACH");
		apilist.add("SendMessageUnreach");
		apilist.add("Oracle OpenFromInitializationString");
		apilist.add("JobID map count");
		apilist.add("CacheJobID");
		apilist.add("GroupID null");
		apilist.add("CheckDBError");
		
		
		
		apilist.add("RequestReSendMessage");
		apilist.add("Check Spam");
		apilist.add("X same message");
		apilist.add("CheckFixedCallbackSpam");
		apilist.add("StatusText");
		apilist.add("Send aborted");
		apilist.add("Send OnProcessReport");
		apilist.add("Not registed CallbackNo");
		apilist.add("OnResetTimer");
		apilist.add("CheckReceiver Invalid callbacknum");
		apilist.add("PushNCheckCMSGID");
		apilist.add("Queue Delayed cnt");
		apilist.add("[SPAMFile]");
		apilist.add("CacheFixedCallbackNo");
		apilist.add("Query SCP::Login");
		apilist.add("QuerySelectJobID");
		apilist.add("QuerySelectInnerNPDB");
		apilist.add("QuerySelectM_SPAM");
		apilist.add("QuerySelectM_SPAM_EU");
		apilist.add("QuerySelectP_GROUP_USER");
		apilist.add("QueryUpdateRetrySend");
		apilist.add("QuerySelectSUSER");
		apilist.add("QueryUpdateMessageReportSend");
		apilist.add("QueryInsertM_UNREACH");
		apilist.add("QueryInsertXMessageSpamSP");
		apilist.add("QueryUpdateRecMessage");
		apilist.add("QueryInsertM_CMSG_KB");
		apilist.add("QueryInsertPhishingLog");
		
		//2018 05-24 newly added
		apilist.add("Not Found SPID");
		apilist.add("First message timeout");
		apilist.add("QuerySelectUSER_GROUPID");
		apilist.add("OnRequestSendReserveMessage");
		apilist.add("QueryUpdateMessageReport");
		apilist.add("예약으로 처리됨 - 예약시간");
		apilist.add("Socket send request queued");
		apilist.add("Socket I/O error");
		apilist.add("Invalid callbacknum");
		apilist.add("Unreach Arrival msg map count");
		apilist.add("OnResponseArrivalMessage");
		apilist.add("flush count");
		apilist.add("Convert file");
		apilist.add("Spam message");
		apilist.add("QuerySelectPhishingCallbackNo");
		apilist.add("Ping Timeout");
		apilist.add("Sent :");
		
		//2018 10-05 newly added
		apilist.add("Failed SendJob");
		
		//apilist.add("h,false)");
		//apilist.add("FileName");
		apilist_eject.add("OnIoComplete");
	//	apilist_eject.add("GroupID null");
//		apilist_eject.add("IsSpam");
		
	}
	boolean checkeject(String str)
	{
		if(apilist_eject.contains(str))
			return true;
		return false;
	}
	String getFirstToken(char start, char end, String str)
	{
		int endidx = str.indexOf(end);
		int startidx = -1;
		for(int i = endidx;i>=0;i--) {
			if(str.charAt(i) == start)
			{
				startidx = i;
				break;
			}
		}

		if(startidx == -1 || endidx == -1 || startidx>endidx)
			return "";
		return str.substring(startidx+1, endidx);
	}
	String getFirstToken(String start, char end, String str)
	{
		int startidx = str.indexOf(start);
		int endidx = -1;
		if(startidx == -1) return "";
		for(int i = startidx + start.length() ;i<str.length();i++) {
			if(str.charAt(i) == end)
			{
				endidx = i;
				break;
			}
		}
		if(end == '\n')
			endidx = str.length();

		if(startidx == -1 || endidx == -1 || startidx>endidx)
			return "";
		//System.out.println(str + " " + start + " " + end + startidx +" " +endidx);
		if(str.substring(startidx+start.length(),endidx).equals("true") || str.substring(startidx+start.length(), endidx).equals("false") )
			return "";
		return str.substring(startidx+start.length(), endidx);
	}
	String getFirstToken(String start, String end, String str)
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
	String getMatchedApi(String str)
	{
		for(int i =0; i<apilist.size(); i++)
		{
			String strapi = apilist.get(i);
			if(str.contains(strapi))
			{
				return strapi;
			}
		}			
		return "";
	}
	void addtype(String type)
	{
		System.out.println("New API added : " + type);
		apilist.add(type);
	}
	
	public static String hashPrivateInfo(String str) {
		String regex = "((010|0303|016|02|031|032|033|041|042|043|051|052|053|054|055|061|062|063|064|067)(\\d{4}|\\d{3})(\\d{4}))|((1588|1577|1600)(\\d{4}))";
		Pattern p  = Pattern.compile(regex);
		Matcher matcher = p.matcher(str);
		while(matcher.find()) {
			String pNumber = str.substring(matcher.start(), matcher.end());
			str = str.replace(pNumber, HashUtil.sha1(pNumber));
			matcher = p.matcher(str);
			//System.out.println("["+test+"]");
			//System.out.println("["+HashUtil.sha1(test)+"]");
		}
		
		return str;
	}
	
	public DataForm ParsebyLine(String line)
	{
		//examples
		//[11:00:56.922][MRSC_/119.205.196.134:11099][Success] OnIoComplete : 807 bytes received
		//[11:00:56.922][MRSC_/119.205.196.134:11099][Info] XML Received
		//[11:00:56.922][][Success] [Inner]	SendReport(Report(SP_ID:dongbu1001, EU_ID:dongbu1001, jobID:66072232521, C_MsgID:m1urmci-x-1-h6hvb-ZG9uZ2J1MjAxMA==), unreach:false, send to EU:true)
		//[11:00:56.922][][Success] [Peer:164]	SendReport(jobID:66072232521, result:0, bSp:false, sp:dongbu1001, eu:dongbu1001)
		//[11:00:56.938][][Info] [check 3325] _OnRequestSendMessage2() - c_msg_id:978721927
		
		//time parsing
//		int
		String linet = line;
		if(linet==null || linet.length() == 0 )
			return null;
		if(linet.charAt(0) != '[')
		{
			int fidx = linet.indexOf('[');
			if(fidx != -1)
				linet = linet.substring(fidx, linet.length());
			else
			{
				//System.out.println("invalid line : " + line);
				return null;
			}
					
		}
		DataForm data = new DataForm();
	/*	StringTokenizer tk = new StringTokenizer(linet,"[]");
		DataForm data = new DataForm();
		
		//time
		if(tk.hasMoreElements())
		{
			String timeelement = (String) tk.nextElement();
			data.setTime(timeelement);
		}
		
		//hostinfo
		if(tk.hasMoreElements())
		{
			String host = (String) tk.nextElement();
			data.setHost(host);
		}
		
		
		//result
		if(tk.hasMoreElements())
		{
			String result = (String) tk.nextElement();
			data.setResult(result);
		}*/
		
		data.setTime(getFirstToken("[",']',linet));
		if(linet.indexOf(']') != -1)
			linet = linet.substring(linet.indexOf(']')+1);
		data.setHost(getFirstToken("[",']',linet));
		if(linet.indexOf(']') != -1)
			linet = linet.substring(linet.indexOf(']')+1);
		data.setResult(getFirstToken("[",']',linet));
		//if(linet.indexOf(']') != -1)
			//linet = linet.substring(linet.indexOf(']')+1);
		
		//type select
		String strtype = getFirstToken('\t','(',linet);
		if(strtype == "")
		{
			if(linet.contains("QueryDeleteMessageExt"))
			{
				//	System.out.println("@@@");
			}
			strtype = getMatchedApi(linet);
			
			if(strtype == "")
			{
				// Send :  , XML Received case
			/*	if(linet.contains("Send :"))
					strtype = "XMLSend";
				if(linet.contains("XML Received"))
					strtype = "XMLRecived";
				*/
				System.out.println("unknown type - "+ line);
				return null;
			}
		}
		else {
	
			if(apilist.contains(strtype)) 
			{
				
			}
			else
			{
				System.out.println(" new api addedd " + strtype);
				addtype(strtype);
			}
		}
		
		int idx = apilist.indexOf(strtype);
		if(visit[idx] == false)
		{
			//System.out.println("type = " + strtype + "    line = " + line);
			visit[idx] = true;
		}
		
		data.setType(strtype);
		if(checkeject(strtype))
			return null;
		
		data.setJobID(getFirstToken("jobID:","),",linet));
		if(data.getJobID() == "" )
			data.setJobID(getFirstToken("jobid:","),",linet));
		
		data.setCustomMessageID(getFirstToken("c_msg_id:"," ),",linet));
		if(data.getCustomMessageID() == "")
			data.setCustomMessageID(getFirstToken("C_MsgID:"," ),",linet));
		if(data.getCustomMessageID() == "")
			data.setCustomMessageID(getFirstToken("c_msg_id:",'\n',linet));

		data.setSpid(getFirstToken(" sp:",",) ",linet));
		if(data.getSpid() == "")
			data.setSpid(getFirstToken(" SP:",",) ",linet));
				
		data.setLogID(LogCounter.count);
		LogCounter.count++;
		LogCounter.count_normal++;
		return data;
	}

	String parseElement(String str, String tag)
	{
		String strstart = "<" + tag + ">";
		String strend = "</" + tag + ">";
		int startidx=-1;
		int endidx=-1;
		startidx = str.indexOf(strstart);
		endidx = str.indexOf(strend);
		if(startidx == -1 || endidx == -1 || startidx>endidx)
			return "";
		return str.substring(startidx + strstart.length(), endidx);		
	}
	public DataForm Parsebyxml(String[] lines, int length)
	{
		/*	<RCP method="consume">
		  <JobID>66072232606</JobID>
		  <Priority>100</Priority>
		  <REQSMS>
		    <SequenceNumber>72232606</SequenceNumber>
		    <Priority>100</Priority>
		    <MessageType>sms</MessageType>
		    <IsSearchNPDB>1</IsSearchNPDB>
		    <SendNumber>030309010171</SendNumber>
		    <ReceiveNumber>01041042961</ReceiveNumber>
		    <CallbackNumber>15888100</CallbackNumber>
		    <Msg></Msg>
		    <Base64Msg>t9S1pTEqOSogvcLAziDB+Cq5zCAyMyw1MDC/+CDAz73DutIgMDQvMDggMTE6MDAgMTG5+LChILSpwPs0NjIsNjQwv/ggICAgICAgICAgICA=</Base64Msg>
		    <CallbackUrl></CallbackUrl>
		    <RD>01990000000</RD>
		    <SP_ID>ldcclcard01</SP_ID>
		    <EU_ID>ldcclcard01</EU_ID>
		    <GRP_ID></GRP_ID>
		    <C_MSG_ID>758595208#!900222</C_MSG_ID>
		    <SUBJECT></SUBJECT>
		    <VCID>3</VCID>
		    <SUB_GRP_ID>1</SUB_GRP_ID>
		    <SubmitTime>20180408110056</SubmitTime>
		    <ExpireTime>20180408140056</ExpireTime>
		    <MMS_TYPE>0</MMS_TYPE>
		    <RETRY_COUNT>0</RETRY_COUNT>
		    <TECOINFO>3</TECOINFO>
		    <Content1></Content1>
		    <Content2></Content2>
		    <Content3></Content3>
		    <Content4></Content4>
		    <CDR></CDR>
		    <PayTime></PayTime>
		    <MessageSubType>1</MessageSubType>
		    <ROUTE_HISTORY smpptype="3">LGT-SMS</ROUTE_HISTORY>
		    <CPID></CPID>
		  </REQSMS>
		</RCP>*/
		
		DataForm data = new DataForm();
		String type = "xml";
		
		for(int i =0;i<length;i++)
		{
			//System.out.println(lines[i]);
			if(lines[i].contains("JobID"))
			{
				data.setJobID(parseElement(lines[i],"JobID"));
			}
			if(lines[i].contains("C_MSG_ID"))
			{
				data.setCustomMessageID(parseElement(lines[i],"C_MSG_ID"));
			}
			if(lines[i].contains("CustomMessageID"))
			{
				data.setCustomMessageID(parseElement(lines[i],"CustomMessageID"));
			}
			if(lines[i].contains("ServiceProviderID"))
			{
				data.setSpid(parseElement(lines[i],"ServiceProviderID"));
			}
			if(lines[i].contains("SP_ID"))
			{
				data.setSpid(parseElement(lines[i],"SP_ID"));
			}
			if(lines[i].contains("Result"))
			{
				data.setResult(parseElement(lines[i],"Result"));
			}
			if(lines[i].contains("ReceiveNumber"))
			{
				String recivenum = parseElement(lines[i],"ReceiveNumber");
				recivenum = hashPrivateInfo(recivenum);
				
				data.setRecivenumber(recivenum);
			}			
			if(lines[i].contains("CallbackNumber"))
			{
				String callbacknum = parseElement(lines[i],"CallbackNumber");
				callbacknum = hashPrivateInfo(callbacknum);
				
				data.setCallbacknumber(callbacknum);
			}
	/*		if(lines[i].contains("SubmitTime"))
			{
				String submittime = parseElement(lines[i],"SubmitTime");
				//submittime = hashPrivateInfo(submittime);
				//20180408110056
				if(submittime.length() == 14)
				{
					
					String strformat = submittime.substring(8, 10) + ":" + submittime.substring(10, 12) + ":"+submittime.substring(12, 14) + ".000";
					//System.out.println(strformat);
					data.setTime(strformat);
				}
			}*/
			
			
			if(lines[i].contains("method=\""))
			{
				int idx = lines[i].indexOf("method=\"");
				String header = "";
				String tail = "";
				int j;
				for(j=idx;j>=0;j--)
				{
					if(lines[i].charAt(j) == '<')
					{
						header = lines[i].substring(j+1, idx -1);
						break;
					}
				}
				if(j == -1)
					System.out.println("Type parse error : " + lines[i]);
				for(j=idx+8;j<lines[i].length() ;j++)
				{
					if(lines[i].charAt(j) == '\"')
					{
						tail = lines[i].substring(idx + 8 , j);
						break;
					}				
				}
				if(j == lines[i].length())
					System.out.println("Type parse error : " + lines[i]);
				
				type = type + "-" + header + "-" + tail;			
				if(tail.equals("consume"))
					type = type + "-";
			}
			if(lines[i].contains("<REQSMS>") )
				type = type + "REQSMS";
			if(lines[i].contains("<REQQUERY>"))
				type = type + "REQQUERY";
		}
	
		
		if(apilist.contains(type))
		{
			
		}
		else
		{
			//System.out.println(type);
		//	System.out.println(lines[0]);
			//System.out.println(lines[1]);
			if(type.equals("xml"))
			{
				for(int i=0;i<length;i++)
					System.out.println(lines[i]);
			}
				
			apilist.add(type);
		}
		
		data.setType( type );
		/*
		String xmlString = "";
		for(int i =0;i<length;i++)
		{
			System.out.println(lines[i]);
			int idx = lines[i].indexOf("</RCP>");
			
			if(idx !=-1)
			{
				lines[i] = lines[i].substring(0, idx+1);
			}
			idx = idx = lines[i].indexOf("</MAS>");
			xmlString += lines[i];

		}
		
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();  
		DocumentBuilder builder;  
		try {  
		    builder = factory.newDocumentBuilder();  
		    Document document = builder.parse(new InputSource(new StringReader(xmlString))); 
		    NodeList nList = document.getElementsByTagName("RCP");
		    Node nNode = nList.item(0);
		    Element eElement = (Element) nNode;
		    String strtype = "xml-" + eElement.getAttribute("method");
		    String strjobid = eElement.getElementsByTagName("JobID").item(0).getTextContent();
		    System.out.println(strtype + "\n" + strjobid);
//		    String strjobid = eElement.getElementsByTagName("JobID").item(0).getTextContent();
		    data.setType(strtype);
		    data.setJobID(strjobid);
		    
		} catch (Exception e) {  
		    e.printStackTrace();  
		    System.exit(0);
		    return null;

		} */
		data.setLogID(LogCounter.count);
		//System.out.println(data.toString());
//		System.exit(0);
		LogCounter.count++;
		LogCounter.count_xml++;
		if(data.getTime().equals("") == true)
			data.setTime(LogCounter.lastTime);
			
		return data;
	}
	
}

