package test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.util.IOUtils;

import cls.Prop;
import lombok.val;
import ot.utils.Basic;

public class Test extends Basic {

	public static void main(String[] args) {
		// TODO 自動生成されたメソッド・スタブ
		out("start");

		test02();
//		test01();

		out("end");
	}

	public static void test02() {
		try {
			val aws = new AwsFile();
			ObjectMetadata om = aws.getInfo("test2.txt");
//			out(om.getContentLength());
			
			val sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			
			// ｻｲｽﾞ
			out(om.getContentLength());
			// 最終更新日付
			out(om.getLastModified());
			out(sdf.format(om.getLastModified()));
		}catch(Exception e) {
			out(e);
		}
		
	}
	// aws write test
	public static void test01() {
		try {
			val aws = new AwsFile();
			val file = new File("tmpfiles\\test2.txt");
			
			try(val is = new FileInputStream(file)){
				aws.write(is, file.getName());
			}
		} catch (Exception e) {
			out(e);
		}
	}
}

class AwsFile extends Basic{
	AmazonS3 client = null;
	String BUCKETNAME = "s3testot";

	public AwsFile() throws Exception {
		val prop = new Prop();
		// proxy環境の場合は、ClientConfigurationを設定する
		val conf = new ClientConfiguration();
		conf.setProtocol(Protocol.HTTPS);
		conf.setProxyHost(prop.get("proxyHost"));
		conf.setProxyPort(Integer.parseInt(prop.get("proxyPort")));

		String accessKey = prop.get("accessKey");
		String secretKey = prop.get("secretKey");
		AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);

		String serviceEndpoint = prop.get("serviceEndpoint");
		String signingRegion = prop.get("signingRegion");
		EndpointConfiguration endpointConfiguration = new EndpointConfiguration(serviceEndpoint, signingRegion);

		client = AmazonS3ClientBuilder.standard()
				.withClientConfiguration(conf)
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withEndpointConfiguration(endpointConfiguration)
				.build();
	}

	public void write(final InputStream is, final String path) throws Exception{
		val om = new ObjectMetadata();
		// isのサイズ取得
		byte[] bytes = IOUtils.toByteArray(is);
		om.setContentLength(bytes.length);
		val bais = new ByteArrayInputStream(bytes);
		// アップロード処理
		val putRequest = new PutObjectRequest(BUCKETNAME, path, bais, om);
		client.putObject(putRequest);
	}
	
//	public Map<String, String> getInfo(final String path){
	public ObjectMetadata getInfo(final String path) {
		val rtnMap = new HashMap<String,String>();
		
		val meta = client.getObjectMetadata(BUCKETNAME, path);
		
		return meta;
	}
}