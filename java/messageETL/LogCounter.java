package messageETL;

public class LogCounter {

	static public long count = 0;
	static public long count_normal = 0;
	static public long count_xml = 0;
	static public String jobid_header = "";
	static public String lastTime = "";
	
	static public void initialize()
	{
		 count = 0;
		count_normal = 0;
		count_xml = 0;
		jobid_header = "";
		lastTime = "";
	}
	public static void SetLastTime(String _lastTime){
		if(_lastTime.length()>12) {
			lastTime =_lastTime.substring(_lastTime.length()-12, _lastTime.length());
		}else lastTime = _lastTime;
	}
}
