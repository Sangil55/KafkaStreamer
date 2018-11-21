package messageETL;

enum logType
{
	rcp_consume ,
	mas_reqsendmessage,
	
}
public class DataForm {
	long logID=0;
	String jobID="";
	String customMessageID="";
	//logType ltype;
	String type="";
	String time="";
	String result="";
	String host="";
	String spid="";
	String recivenumber ="";
	String masHost="";
	public String getMasHost() {
		return masHost;
	}
	public void setMasHost(String masHost) {
		this.masHost = masHost;
	}
	public String getRecivenumber() {
		return recivenumber;
	}
	public void setRecivenumber(String recivenumber) {
		this.recivenumber = recivenumber;
	}
	public String getCallbacknumber() {
		return callbacknumber;
	}
	public void setCallbacknumber(String callbacknumber) {
		this.callbacknumber = callbacknumber;
	}
	String callbacknumber ="";
	long parentID=-1;
	
	public String getSpid() {
		return spid;
	}
	public void setSpid(String spid) {
		this.spid = spid;
	}
	
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	
	@Override
	public String toString() {
		return "DataForm [logID=" + logID + ", jobID=" + jobID + ", customMessageID=" + customMessageID +
				", type=" + type + ", time=" + time + ", result=" + result + ", host=" + host + ", parentID="
				+ parentID + "]";
	}
	public long getLogID() {
		return logID;
	}
	public void setLogID(long logID) {
		this.logID = logID;
	}
	public String getJobID() {
		return jobID;
	}
	public void setJobID(String jobID) {
		// usual case 66072232630
		// unuual case 072232630
		String newjobid = jobID;
		//setting jobid with last 9 digits
		if(jobID.length() >= 11)
		{
			newjobid = jobID.substring(jobID.length() - 9 , jobID.length());
		}

		this.jobID = newjobid;
	}
	public String getCustomMessageID() {
		return customMessageID;
	}
	public void setCustomMessageID(String customMessageID) {
		this.customMessageID = customMessageID;
	}
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getTime() {
		return time;
	}
	
	public void setTime(String time) {
		LogCounter.SetLastTime(time);
		this.time = time;
	}
	public String getResult() {
		return result;
	}
	public void setResult(String result) {
		this.result = result;
	}
	public long getParentID() {
		return parentID;
	}
	public void setParentID(long parentID) {
		this.parentID = parentID;
	}
}
