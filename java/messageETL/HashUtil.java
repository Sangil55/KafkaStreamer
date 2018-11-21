package messageETL;

import java.security.MessageDigest;

public class HashUtil {
	public static String sha1(String plain) {
		return shaHashing("SHA-1", plain);
	}
	
	public static String sha256(String plain) {
		return shaHashing("SHA-256", plain);
	}
	
	public static String shaHashing(String shaAlgorithm, String plain) {
		String result="";
		try {
			MessageDigest md = MessageDigest.getInstance(shaAlgorithm);
			md.update(plain.getBytes());
			byte byteData[] = md.digest();
			StringBuffer sb = new StringBuffer();
			for(int i = 0; i < byteData.length ; i++) {
				sb.append(Integer.toString((byteData[i]&0xff) + 0x100, 16).substring(1));
			}
			result = sb.toString();
		}catch(Exception e ) {
			e.printStackTrace();
		}
		return result;
	}
}
