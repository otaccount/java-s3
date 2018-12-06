package cls;

import java.io.FileInputStream;
import java.util.Properties;

import com.amazonaws.util.StringUtils;

import lombok.val;

public class Prop {
	Properties prop;
	String file = "resources/data.properties";
	
	public Prop() throws Exception{
		prop = new Properties();
		try(val is = new FileInputStream(file)){
			prop.load(is);
		}catch(Exception e) {
			throw e;
		}
	}

	public String get(String key) throws Exception{
		val value = prop.getProperty(key);
		
		if(StringUtils.isNullOrEmpty(value)) {
			throw new Exception("無効なkey:" + key);
		}
		return value;
	}
}