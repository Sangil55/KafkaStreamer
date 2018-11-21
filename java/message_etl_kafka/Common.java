package message_etl_kafka;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.LogManager;

public class Common {
	private org.apache.logging.log4j.Logger logger = LogManager.getLogger(Common.class.getName());
	
	/**
	 * @brief 설정파일 로드
	 * 
	 * config/common.properties 파일을 읽어들임 
	 * @return Properties 객체
	 */
	public Properties readProperty() {
		Properties prop = new Properties();
		try {
			String path = new java.io.File(".").getCanonicalPath();
			logger.info("설정 파일 읽는 중.. 경로: " + path);
			FileInputStream file = new FileInputStream(path+"/config/common.properties");
			prop.load(file);
			printProperties(prop);
			logger.info("설정파일 읽기 완료");
		} catch (IOException e) {
			logger.error("설정파일을 여는 중 에러가 발생했습니다.");
			e.printStackTrace();
		}
		return prop;
	}
	
	/**
	 * @brief 설정파일 내용 출력
	 * @param prop
	 */
	public void printProperties(Properties prop) {
	logger.info("=========== Print Properties ==========");
		Set<Object> keyset = prop.keySet();
		Iterator<Object> keyiter = keyset.iterator();
		
		while(keyiter.hasNext()) {
			String propKey = keyiter.next().toString();
			String propVal = prop.getProperty(propKey);
			logger.info("* key : "+propKey + ", value : "+propVal);
		}
	}
}
