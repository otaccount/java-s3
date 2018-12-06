package org.opengion.plugin.cloud;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpSession;

import org.apache.commons.lang3.StringUtils;
import org.opengion.fukurou.util.Closer;
import org.opengion.fukurou.util.FileUtil;
import org.opengion.hayabusa.common.HybsSystem;
import org.opengion.hayabusa.common.HybsSystemException;
import org.opengion.hayabusa.io.StorageAPI;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.microsoft.azure.storage.blob.BlobOutputStream;

/**
 * aws用のクラウドストレージ操作実装
 *
 * システムリソースのS3_ACCESS_KEY,S3_SECRET_KEY,S3_SERVICE_END_POINT,S3_REGIONに、AWSのキー情報を登録する必要があります。
 * （IAMを利用する場合には認証情報を登録する必要はありません）
 * 
 * また、Edit機能のファイル出力を利用する場合はS3上(例えばvar/lib/tomcat8/webapps/ge/jsp/common)に
 * fileDownloadListDef.txtをアップロードしておく必要があります。
 *
 * @og.group クラウド
 * @og.rev  (2018/02/15) 新規作成
 * @og.rev 5.9.32.1 (2018/05/11) パスの先頭が「/」の場合は「/」の除去と、「//」を「/」に置換処理の追加。
 *
 * @version 5.0
 * @author T.OTA
 * @sinse JDK7.0
 */
public class StorageAPI_aws implements StorageAPI {
	// 認証文字列
	// ｱｸｾｽｷｰ
	private String s3AccessKey = "";
	// ｼｰｸﾚｯﾄｷｰ
	private String s3SecretKey = "";
	// ｴﾝﾄﾞﾎﾟｲﾝﾄ
	private String s3ServiceEndPoint = "";
	// ﾚｷﾞｵﾝ
	private String s3Region = "";
	// ﾊﾞｹｯﾄ名(ｺﾝﾃﾅ名)
	String s3bucket = "";

	AmazonS3 client = null;

	/**
	 * コンストラクタ
	 *
	 * @param container
	 * @param hsession
	 */
	public StorageAPI_aws(String container, HttpSession hsession){
		// ﾘｿｰｽﾊﾟﾗﾒｰﾀ設定
		// ｱｸｾｽｷｰ
		s3AccessKey = HybsSystem.sys("CLOUD_STORAGE_S3_ACCESS_KEY");
    	// コンテナ名をs3bucketとして保持しておく
    	s3bucket = container;

    	// S3アクセスクライアントの生成
    	if(StringUtils.isEmpty(s3AccessKey)){
        	// IAMロールによる認証
	    	client = AmazonS3ClientBuilder.standard()
	    			.withCredentials(new InstanceProfileCredentialsProvider(false))
	    			.build();
    	}else {
	    	// リソースのアクセスキーによる認証
    		// ｼｰｸﾚｯﾄｷｰ
    		s3SecretKey = HybsSystem.sys("CLOUD_STORAGE_S3_SECRET_KEY");
    		// ｴﾝﾄﾞﾎﾟｲﾝﾄ
    		s3ServiceEndPoint = HybsSystem.sys("CLOUD_STORAGE_S3_SERVICE_END_POINT");
    		// ﾚｷﾞｵﾝ
    		s3Region = HybsSystem.sys("CLOUD_STORAGE_S3_REGION");

			// 初期ﾁｪｯｸ
			initCheck();

	    	// AWSの認証情報
	    	AWSCredentials credentials = new BasicAWSCredentials(s3AccessKey, s3SecretKey);

	    	// エンドポイント設定
	    	EndpointConfiguration endpointConfiguration = new EndpointConfiguration(s3ServiceEndPoint,  s3Region);
	    	client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials))
	    			.withEndpointConfiguration(endpointConfiguration).build();
    	}

    	// S3に指定されたﾊﾞｹｯﾄ(ｺﾝﾃﾅ)が存在しない場合は、作成する
    	if(!client.doesBucketExist(container)){
    		client.createBucket(container);
    	}
	}

	/**
	 * 初期ﾁｪｯｸ
	 */
	private void initCheck(){
		// ｼｽﾃﾑﾘｿｰｽに認証情報が登録されていない場合は、エラー
		StringBuilder errString = new StringBuilder();
		if(StringUtils.isEmpty(s3AccessKey)){
			errString.append("CLOUD_STORAGE_S3_ACCESS_KEY");
		}
		if(StringUtils.isEmpty(s3SecretKey)){
			errString.append(",CLOUD_STORAGE_S3_SECRET_KEY");
		}
		if(StringUtils.isEmpty(s3ServiceEndPoint)){
			errString.append(",CLOUD_STORAGE_S3_SERVICE_END_POINT");
		}
		if(StringUtils.isEmpty(s3Region)){
			errString.append(",CLOUD_STORAGE_S3_REGION");
		}

		if(errString.length() > 0){
			throw new HybsSystemException("AWSのｷｰ情報("+errString.toString()+")がｼｽﾃﾑﾘｿｰｽに登録されていません。");
		}

	}

	/**
	 * ファイルパスの編集 2018/05/07 ADD
	 * パスの先頭が「/」の場合は「/」の除去と、「//」を「/」に置換処理の追加。
	 * 
	 * @og.rev 5.9.32.1 (2018/05/11)
	 * @param path
	 * @return 変更後パス
	 */
	private String editPath(String path) {
		// 先頭が「/」の場合は除去
		if("/".equals(path.substring(0, 1))) {
			path = path.substring(1);
		}
		// 「//」は「/」に置換
		path = path.replaceAll("//", "/");

		return path;
	}

	/**
	 * ｱｯﾌﾟﾛｰﾄﾞ
	 *
	 * @og.rev 5.9.32.1 (2018/05/11)
	 * @param partInputStream	ｱｯﾌﾟﾛｰﾄﾞ対象のｽﾄﾘｰﾑ
	 * @param updFolder		ｱｯﾌﾟﾛｰﾄﾞﾌｫﾙﾀ名
	 * @param updFileName		ｱｯﾌﾟﾛｰﾄﾞﾌｧｲﾙ名
	 * @param hsession			ｾｯｼｮﾝ
	 */
	@Override
	public void add(InputStream partInputStream, String updFolder, String updFileName, HttpSession hsession) {
		BlobOutputStream blobOutputStream = null;
		try {
			// 2018/05/07 ADD
			updFolder = editPath(updFolder);

			// ｱｯﾌﾟﾛｰﾄﾞ処理
			ObjectMetadata om = new ObjectMetadata();

			final PutObjectRequest putRequest = new PutObjectRequest(s3bucket, updFolder + updFileName, partInputStream,om);
			// ｱｯﾌﾟﾛｰﾄﾞ実行
			client.putObject(putRequest);

		} catch (Exception e) {
			StringBuilder sbErrMsg = new StringBuilder();
			sbErrMsg.append("ストレージへのファイルアップロードに失敗しました。updFolder:");
			sbErrMsg.append(updFolder);
			sbErrMsg.append(" updFileName:");
			sbErrMsg.append(updFileName);
			sbErrMsg.append(" errInfo:");
			sbErrMsg.append(e);
			throw new HybsSystemException(sbErrMsg.toString());
		} finally {
			// ｸﾛｰｽﾞ処理
			Closer.ioClose(blobOutputStream);
			Closer.ioClose(partInputStream);
		}
	}

	/**
	 * ﾀﾞｳﾝﾛｰﾄﾞ
	 *
	 * @og.rev 5.9.32.1 (2018/05/11)
	 * @param filePath	ﾀﾞｳﾝﾛｰﾄﾞ対象のﾌｧｲﾙﾊﾟｽ
	 * @param hsession	ｾｯｼｮﾝ
	 * @return ストリーム
	 */
	@Override
	public InputStream get(String filePath, HttpSession hsession) {
		InputStream is = null;
		// ﾀﾞｳﾝﾛｰﾄﾞ
		try {
			// 2018/05/07 ADD
			filePath = editPath(filePath);

			S3Object object = client.getObject(s3bucket, filePath);

			is = object.getObjectContent();
		} catch (Exception e) {
			StringBuilder sbErrMsg = new StringBuilder();
			sbErrMsg.append("ストレージからのファイルダウンロードに失敗しました。filePath:");
			sbErrMsg.append(filePath);
			sbErrMsg.append(" errInfo:");
			sbErrMsg.append(e);
			throw new HybsSystemException(sbErrMsg.toString());
		}

		return is;
	}

	/**
	 * ｺﾋﾟｰ
	 *
	 * @og.rev 5.9.32.1 (2018/05/11)
	 * @param oldFilePath	ｺﾋﾟｰ元ﾌｧｲﾙﾊﾟｽ
	 * @param newFilePath	ｺﾋﾟｰ先ﾌｧｲﾙﾊﾟｽ
	 * @param hsession		ｾｯｼｮﾝ
	 */
	@Override
	public void copy(String oldFilePath, String newFilePath, HttpSession hsession) {
		try {
			// 2018/05/07 ADD
			oldFilePath = editPath(oldFilePath);
			newFilePath = editPath(newFilePath);

			final CopyObjectRequest copyRequest = new CopyObjectRequest(s3bucket, oldFilePath, s3bucket, newFilePath);
			client.copyObject(copyRequest);
		} catch (Exception e) {
			StringBuilder sbErrMsg = new StringBuilder();
			sbErrMsg.append("ストレージのファイルコピー処理に失敗しました。oldFilePath:");
			sbErrMsg.append(oldFilePath);
			sbErrMsg.append(" newFilePath:");
			sbErrMsg.append(newFilePath);
			sbErrMsg.append(" errInfo:");
			sbErrMsg.append(e);
			throw new HybsSystemException(sbErrMsg.toString());
		}
	}

	/**
	 * 削除
	 * 
	 * @og.rev 5.9.32.1 (2018/05/11)
	 * @param filePath	削除ﾌｧｲﾙのﾊﾟｽ
	 * @param hsession	ｾｯｼｮﾝ
	 */
	@Override
	public void delete(String filePath, HttpSession hsession) {
		// 削除
		try {
			// 2018/05/07 ADD
			filePath = editPath(filePath);

			final DeleteObjectRequest deleteRequest = new DeleteObjectRequest(s3bucket, filePath);
			client.deleteObject(deleteRequest);
			client.deleteObject(s3bucket, filePath);
		} catch (Exception e) {
			StringBuilder sbErrMsg = new StringBuilder();
			sbErrMsg.append("ストレージのファイル削除に失敗しました。filePath:");
			sbErrMsg.append(filePath);
			sbErrMsg.append(" errInfo:");
			sbErrMsg.append(e);
			throw new HybsSystemException(sbErrMsg.toString());
		}
	}

	/**
	 * ﾌｧｲﾙ名変更
	 *
	 * @param filePath		ﾌｧｲﾙﾊﾟｽ
	 * @param oldFileName	変更前ﾌｧｲﾙ名
	 * @param newFileName	変更後ﾌｧｲﾙ名
	 * @param useBackup	変更後ﾌｧｲﾙ名が既に存在する場合のﾊﾞｯｸｱｯﾌﾟ作成ﾌﾗｸﾞ
	 * @param hsession		ｾｯｼｮﾝ
	 */
	public void rename(String filePath, String oldFileName, String newFileName, final boolean useBackup,
			HttpSession hsession) {
		String newFilePath = filePath + newFileName;
		String oldFilePath = filePath + oldFileName;

		// 変更先のファイルが存在した場合の処理
		if (exists(newFilePath, hsession)) {
			// バックアップ作成する場合
			if (useBackup) {
				// バックアップファイル名は、元のファイル名(拡張子含む) ＋ "_" + 現在時刻のlong値 + "." +
				// 元のファイルの拡張子
				String bkupPath = filePath + "_backup/" + newFileName + "_" + System.currentTimeMillis()
						+ FileUtil.EXTENSION_SEPARATOR + FileUtil.getExtension(newFileName);
				// バックアップフォルダに移動
				copy(newFilePath, bkupPath, hsession);
			}
		}

		// コピー
		copy(oldFilePath, newFilePath, hsession);
		// 削除
		delete(oldFilePath, hsession);
	}

	/**
	 * ﾌｧｲﾙ存在ﾁｪｯｸ
	 *
	 * @og.rev 5.9.32.1 (2018/05/11)
	 * @param filePath			ﾌｧｲﾙﾊﾟｽ
	 * @param hsession		ｾｯｼｮﾝ
	 * @return				true:存在 false:存在しない
	 */
//	@Override
	public boolean exists(String filePath, HttpSession hsession) {
		boolean blnRtn = true;
		try {
			// 2018/05/07 ADD
			filePath = editPath(filePath);

			if (!client.doesObjectExist(s3bucket, filePath)) {
				// ﾌｧｲﾙが取得できなかった場合は、falseを設定
				blnRtn = false;
			}
		} catch (Exception e) {
			StringBuilder sbErrMsg = new StringBuilder();
			sbErrMsg.append("ストレージのファイル取得に失敗しました。filePath:");
			sbErrMsg.append(filePath);
			sbErrMsg.append(" errInfo:");
			sbErrMsg.append(e);
			throw new HybsSystemException(sbErrMsg.toString());
		}

		return blnRtn;
	}

	/**
	 * ﾌｧｲﾙ一覧取得
	 *
	 * @param startsWith	ﾊﾟｽの前方一致
	 * @param hsession		ｾｯｼｮﾝ
	 * @return				ﾌｧｲﾙﾊﾟｽ一覧
	 */
	@Override
	public String[] list(String startsWith, HttpSession hsession) {
		// 認証
		List<String> rtnList = new ArrayList<String>();
		try{
			// 2018/05/07 ADD
			startsWith = editPath(startsWith);

			ListObjectsV2Request request = new ListObjectsV2Request()
					.withBucketName(s3bucket)
					.withPrefix(startsWith);
			ListObjectsV2Result list = client.listObjectsV2(request);
			List<S3ObjectSummary> objects =  list.getObjectSummaries();
			// 一覧の取得
			for(S3ObjectSummary obj: objects){
				// 名称を格納
				rtnList.add(obj.getKey());
			}
		} catch (Exception e){
			StringBuilder sbErrMsg = new StringBuilder();
			sbErrMsg.append("ファイル一覧の取得に失敗しました。startsWith:");
			sbErrMsg.append(startsWith);
			sbErrMsg.append(" errInfo:");
			sbErrMsg.append("e");
			throw new HybsSystemException(sbErrMsg.toString());
		}
		return rtnList.toArray(new String[rtnList.size()]);
	}

	/**
	 * ﾌｧｲﾙ情報取得
	 *
	 * @og.rev 5.9.32.1 (2018/05/11)
	 * @param path			ﾌｧｲﾙﾊﾟｽ
	 * @param hsession		ｾｯｼｮﾝ
	 * @return				ﾌｧｲﾙ情報格納Map
	 */
//	@Override
	public Map<String, String> getInfo(String path, HttpSession hsession) {
		Map<String, String> rtnMap = new HashMap<String,String>();

		ObjectMetadata meta = null;
		try{
			// 2018/05/07 ADD
			path = editPath(path);

			// ﾌｧｲﾙｵﾌﾞｼﾞｪｸﾄの取得
			meta = client.getObjectMetadata(s3bucket, path);
		}catch(Exception e){
			StringBuilder sbErrMsg = new StringBuilder();
			sbErrMsg.append("ファイルの取得に失敗しました。path:");
			sbErrMsg.append(path);
			sbErrMsg.append(" errInfo:");
			sbErrMsg.append(e);
			throw new HybsSystemException(sbErrMsg.toString());
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");

		// ﾌｧｲﾙｻｲｽﾞ
		rtnMap.put(FILEINFO_SIZE, String.valueOf(meta.getContentLength()));
		// 最終更新時刻
		rtnMap.put(FILEINFO_LASTMODIFIED, sdf.format(meta.getLastModified()));

		return rtnMap;
	}
}
