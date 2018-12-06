package org.opengion.plugin.cloud;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpSession;

import org.opengion.fukurou.util.Closer;
import org.opengion.fukurou.util.FileUtil;
import org.opengion.fukurou.util.StringUtil;
import org.opengion.hayabusa.common.HybsSystem;
import org.opengion.hayabusa.common.HybsSystemException;
import org.opengion.hayabusa.io.StorageAPI;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.BlobOutputStream;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

/**
 * azure用のクラウドストレージ操作実装
 *
 * システムリソースのCLOUD_STORAGE_AZURE_KEYに、Azureのキー情報を登録する必要があります。
 *
 * @og.group クラウド
 * @og.rev 5.9.29.1 (2018/02/09) 新規作成
 *
 * @version 5.0
 * @author T.OTA
 * @sinse JDK7.0
 */
public class StorageAPI_azure implements StorageAPI {
	// 認証文字列
	private CloudBlobContainer blobContainer = null;

	/**
	 * @param container
	 * @param hsession
	 */
	public StorageAPI_azure(String container, HttpSession hsession){
		String storageConnectionString = HybsSystem.sys("CLOUD_STORAGE_AZURE_KEY");
		if(StringUtil.isNull(storageConnectionString)){
			String errMsg = "Azure用認証キーがシステムリソースに登録されていません。";
			throw new HybsSystemException(errMsg);
		}

		try{
			CloudStorageAccount account = CloudStorageAccount.parse(storageConnectionString);
			CloudBlobClient serviceClient = account.createCloudBlobClient();
			blobContainer = serviceClient.getContainerReference(container);
			// コンテナが存在しない場合は作成する
			blobContainer.createIfNotExists();
		}catch(Exception e){
			StringBuilder sbErrMsg = new StringBuilder();
			sbErrMsg.append("コンテナの作成に失敗しました。container:");
			sbErrMsg.append(container);
			throw new HybsSystemException(sbErrMsg.toString());
		}
	}

	/**
	 * ｱｯﾌﾟﾛｰﾄﾞ
	 *
	 * @param partInputStream	ｱｯﾌﾟﾛｰﾄﾞ対象のｽﾄﾘｰﾑ
	 * @param updFolder		ｱｯﾌﾟﾛｰﾄﾞﾌｫﾙﾀ名
	 * @param updFileName		ｱｯﾌﾟﾛｰﾄﾞﾌｧｲﾙ名
	 * @param hsession			ｾｯｼｮﾝ
	 */
	@Override
	public void add(InputStream partInputStream, String updFolder, String updFileName, HttpSession hsession) {
		BlobOutputStream blobOutputStream = null;
		try {
			// ｱｯﾌﾟﾛｰﾄﾞ処理
			CloudBlockBlob blob = blobContainer.getDirectoryReference(updFolder).getBlockBlobReference(updFileName);
			blobOutputStream = blob.openOutputStream();

			byte buffer[]  = new byte[4096];
			int size;

			while((size = partInputStream.read(buffer)) != -1){
				blobOutputStream.write(buffer,0,size);
				blobOutputStream.flush();
			}

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
	 * @param filePath	ﾀﾞｳﾝﾛｰﾄﾞ対象のﾌｧｲﾙﾊﾟｽ
	 * @param hsession	ｾｯｼｮﾝ
	 * @return ストリーム
	 */
	@Override
	public InputStream get(String filePath, HttpSession hsession) {
		CloudBlockBlob blob = null;
		InputStream is = null;
		// ﾀﾞｳﾝﾛｰﾄﾞ
		try {
			blob = blobContainer.getBlockBlobReference(filePath);
			is = blob.openInputStream();
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
	 * @param oldFilePath	ｺﾋﾟｰ元ﾌｧｲﾙﾊﾟｽ
	 * @param newFilePath	ｺﾋﾟｰ先ﾌｧｲﾙﾊﾟｽ
	 * @param hsession		ｾｯｼｮﾝ
	 */
	@Override
	public void copy(String oldFilePath, String newFilePath, HttpSession hsession) {
		// ｺﾋﾟｰ処理
		InputStream is = null;

		try {
			CloudBlockBlob oldblob = blobContainer.getBlockBlobReference(oldFilePath);
			CloudBlockBlob newblob = blobContainer.getBlockBlobReference(newFilePath);
			newblob.startCopy(oldblob);
		} catch (Exception e) {
			StringBuilder sbErrMsg = new StringBuilder();
			sbErrMsg.append("ストレージのファイルコピー処理に失敗しました。oldFilePath:");
			sbErrMsg.append(oldFilePath);
			sbErrMsg.append(" newFilePath:");
			sbErrMsg.append(newFilePath);
			sbErrMsg.append(" errInfo:");
			sbErrMsg.append(e);
			throw new HybsSystemException(sbErrMsg.toString());
		}finally{
			// ｸﾛｰｽﾞ処理
			Closer.ioClose(is);
		}
	}

	/**
	 * 削除
	 *
	 * @param filePath	削除ﾌｧｲﾙのﾊﾟｽ
	 * @param hsession	ｾｯｼｮﾝ
	 */
	@Override
	public void delete(String filePath, HttpSession hsession) {
		// 削除
		try {
			CloudBlockBlob blob = blobContainer.getBlockBlobReference(filePath);
			blob.delete();
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
	 * @param filePath			ﾌｧｲﾙﾊﾟｽ
	 * @param hsession		ｾｯｼｮﾝ
	 * @return				true:存在 false:存在しない
	 */
//	@Override
	public boolean exists(String filePath, HttpSession hsession) {
		boolean blnRtn = true;
		try {
			CloudBlockBlob blob = blobContainer.getBlockBlobReference(filePath);
			if (!blob.exists()) {
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
			// 一覧の取得
			for(ListBlobItem item: blobContainer.listBlobs(startsWith)){
				if(item instanceof CloudBlob){
				// 名称を格納
				rtnList.add(((CloudBlob)item).getName());
				}
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
	 * @param path			ﾌｧｲﾙﾊﾟｽ
	 * @param hsession		ｾｯｼｮﾝ
	 * @return				ﾌｧｲﾙ情報格納Map
	 */
//	@Override
	public Map<String, String> getInfo(String path, HttpSession hsession) {
		Map<String, String> rtnMap = new HashMap<String,String>();

		CloudBlob blob = null;
		try{
			// ﾌｧｲﾙｵﾌﾞｼﾞｪｸﾄの取得
			blob = blobContainer.getBlobReferenceFromServer(path);
		}catch(Exception e){
			StringBuilder sbErrMsg = new StringBuilder();
			sbErrMsg.append("ファイルの取得に失敗しました。path:");
			sbErrMsg.append(path);
			sbErrMsg.append(" errInfo:");
			sbErrMsg.append(e);
			throw new HybsSystemException(sbErrMsg.toString());
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");

		BlobProperties properties =  blob.getProperties();
		// ﾌｧｲﾙｻｲｽﾞ
		rtnMap.put(FILEINFO_SIZE, String.valueOf(properties.getLength()));
		// 最終更新時刻
		rtnMap.put(FILEINFO_LASTMODIFIED, sdf.format(properties.getLastModified()));

		return rtnMap;
	}
}
