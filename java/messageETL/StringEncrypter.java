package messageETL;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;

public class StringEncrypter {
	static String encryptString(String str, String seed)
	{
		StandardPBEStringEncryptor encryptor =  new StandardPBEStringEncryptor();;
		//seed = "swino";
		encryptor.setPassword(seed);
		return encryptor.encrypt(str);
	}
	static String decryptString(String str, String seed)
	{
		StandardPBEStringEncryptor encryptor =  new StandardPBEStringEncryptor();;
		//seed = "swino";
		encryptor.setPassword(seed);
		return encryptor.decrypt(str);
	}
}
