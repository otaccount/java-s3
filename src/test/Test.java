package test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;

import cls.Prop;
import lombok.val;

public class Test{

	public static void main(String[] args) {
		// TODO 自動生成されたメソッド・スタブ

		test03();

	}

	// 一覧取得には
	public static void test03() {
		try {
			val aws = new AwsFile();
			val list = aws.list("tempfiles");
			
			Arrays.asList(list).stream()
				.forEach(System.out::println);
			
		}catch(Exception e) {
			System.out.println(e);
		}
	}
	
	// test02
	public static void test02() {
		try {
			val aws = new AwsFile();
			ObjectMetadata om = aws.getInfo("test2.txt");
//			System.out.println(om.getContentLength());
			
			val sdf = new SimpleDateFormat("yyyyMMddHHmmss");
			
			// ｻｲｽﾞ
			System.out.println(om.getContentLength());
			// 最終更新日付
			System.out.println(om.getLastModified());
			System.out.println(sdf.format(om.getLastModified()));
		}catch(Exception e) {
			System.out.println(e);
		}
		
	}
	// aws write test
	public static void test01() {
		try {
			val aws = new AwsFile();
			val file = new File("tmpfiles\\test2.txt");
			
			try(val is = new FileInputStream(file)){
				aws.write(is, "tempfiles/subdir/" + file.getName());
			}
		} catch (Exception e) {
			System.out.println(e);
		}
	}
}


/** AWS S3 操作クラス */
class AwsFile{
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
	
	/**
	 * ﾌｧｲﾙ一覧取得
	 *
	 * @param startsWith	ﾊﾟｽの前方一致
	 * @param hsession		ｾｯｼｮﾝ
	 * @return				ﾌｧｲﾙﾊﾟｽ一覧
	 */
	public String[] list(String startsWith) {
		// 認証
		List<String> rtnList = new ArrayList<String>();

		ListObjectsV2Request request = new ListObjectsV2Request()
				.withBucketName(BUCKETNAME)
				.withPrefix(startsWith);
		ListObjectsV2Result list = client.listObjectsV2(request);
		List<S3ObjectSummary> objects =  list.getObjectSummaries();
		// 一覧の取得
		for(S3ObjectSummary obj: objects){
			// 名称を格納
			rtnList.add(obj.getKey());
		}
		
		return rtnList.toArray(new String[rtnList.size()]);
	}
	
//	public Map<String, String> getInfo(final String path){
	public ObjectMetadata getInfo(final String path) {
		val rtnMap = new HashMap<String,String>();
		
		val meta = client.getObjectMetadata(BUCKETNAME, path);
		
		return meta;
	}
}