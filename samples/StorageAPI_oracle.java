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

import oracle.cloud.storage.CloudStorage;
import oracle.cloud.storage.CloudStorageConfig;
import oracle.cloud.storage.CloudStorageFactory;
import oracle.cloud.storage.exception.NoSuchContainerException;
import oracle.cloud.storage.exception.NoSuchObjectException;
import oracle.cloud.storage.model.Key;
import oracle.cloud.storage.model.QueryOption;

/**
 * azure用のクラウドストレージ操作実装
 *
 * 以下のシステムリソースに設定が必要です。
 * CLOUD_STORAGE_ORACLE_SERVICE_NAME
 * CLOUD_STORAGE_ORACLE_USERNAME
 * CLOUD_STORAGE_ORACLE_PASSWORD
 * CLOUD_STORAGE_ORACLE_SERVICEURL
 *
 * @og.group クラウド
 * @og.rev 5.9.33.0 (2018/06/01) 新規作成
 *
 * @version 5.0
 * @author T.OTA
 * @sinse JDK7.0
 */
public class StorageAPI_oracle implements StorageAPI {
	// 認証文字列
	// ｻｰﾋﾞｽ名
	private String serviceName = "";
	// ﾕｰｻﾞ名
	private String userName = "";
	// ﾊﾟｽﾜｰﾄﾞ
	private String password = "";
	// ｻｰﾋﾞｽURL
	private String serviceUrl = "";
	// ｺﾝﾃﾅ名
	private String containerNm = "";

	// 下記oracle用
	 CloudStorage myConnection = null;;

	/**
	 * コンストラクタ
	 *
	 * @param container
	 * @param hsession
	 */
	public StorageAPI_oracle(String container, HttpSession hsession){
		CloudStorageConfig myConfig = new CloudStorageConfig();

		// リソースパラメータ設定
		// サービス名
		serviceName = HybsSystem.sys("CLOUD_STORAGE_ORACLE_SERVICE_NAME");
		// ユーザ名
		userName = HybsSystem.sys("CLOUD_STORAGE_ORACLE_USERNAME");
		// パスワード
		password = HybsSystem.sys("CLOUD_STORAGE_ORACLE_PASSWORD");
		// サービスURL
		serviceUrl = HybsSystem.sys("CLOUD_STORAGE_ORACLE_SERVICEURL");

		initCheck();

		// コンテナ名をクラス変数に保持
		containerNm = container;

		try {
			// 接続情報の設定
			myConfig.setServiceName(serviceName)
		    	.setUsername(userName)
		    	.setPassword(password.toCharArray())
		    	.setServiceUrl(serviceUrl);

			myConnection = CloudStorageFactory.getStorage(myConfig);

	    	// コンテナの存在チェック
			try {
				myConnection.describeContainer(containerNm);
			}catch(NoSuchContainerException nce) {
				// コンテナが存在しない場合は、作成する
				myConnection.createContainer(container);
			}
		}catch(Exception e) {
			throw new HybsSystemException(e);
		}
	}

	/**
	 * 初期ﾁｪｯｸ
	 */
	private void initCheck(){
		// ｼｽﾃﾑﾘｿｰｽに認証情報が登録されていない場合は、エラー
		StringBuilder errString = new StringBuilder();
		if(StringUtils.isEmpty(serviceName)){
			errString.append("CLOUD_STORAGE_ORACLE_SERVICE_NAME");
		}
		if(StringUtils.isEmpty(userName)){
			errString.append(",CLOUD_STORAGE_ORACLE_USERNAME");
		}
		if(StringUtils.isEmpty(password)){
			errString.append(",CLOUD_STORAGE_ORACLE_PASSWORD");
		}
		if(StringUtils.isEmpty(serviceUrl)){
			errString.append(",CLOUD_STORAGE_ORACLE_SERVICEURL");
		}

		if(errString.length() > 0){
			throw new HybsSystemException("クラウドストレージのキー情報("+errString.toString()+")がシステムリソースに登録されていません。");
		}

	}

	/**
	 * ファイルパスの編集
	 * パスの先頭が「/」の場合は「/」の除去と、「//」を「/」に置換処理の追加。
	 *
	 * @param path
	 * @return
	 */
	private String editPath(String path) {
		// pathが空の場合は空文字を返す
		if(StringUtils.isEmpty(path)) {
			return "";
		}

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
	 * @param partInputStream	ｱｯﾌﾟﾛｰﾄﾞ対象のｽﾄﾘｰﾑ
	 * @param updFolder		ｱｯﾌﾟﾛｰﾄﾞﾌｫﾙﾀ名
	 * @param updFileName		ｱｯﾌﾟﾛｰﾄﾞﾌｧｲﾙ名
	 * @param hsession			ｾｯｼｮﾝ
	 */
	@Override
	public void add(InputStream partInputStream, String updFolder, String updFileName, HttpSession hsession) {
		try {
			// パスをクラウド用に編集
			updFolder = editPath(updFolder);

			myConnection.storeObject(containerNm, updFolder + updFileName, "application/octet-stream", partInputStream);

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
		InputStream is = null;
		// ﾀﾞｳﾝﾛｰﾄﾞ
		try {
			// パスをクラウド用に編集
			filePath = editPath(filePath);

			is = myConnection.retrieveObject(containerNm, filePath);

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
		InputStream is = null;
		try {
			// パスをクラウド用に編集
			oldFilePath = editPath(oldFilePath);
			newFilePath = editPath(newFilePath);

			// コピー元から取得
			is = get(oldFilePath, hsession);
			// コピー先にアップロード
			add(is, newFilePath, "", hsession);

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
	 * @param filePath	削除ﾌｧｲﾙのﾊﾟｽ
	 * @param hsession	ｾｯｼｮﾝ
	 */
	@Override
	public void delete(String filePath, HttpSession hsession) {
		// 削除
		try {
			// パスをクラウド用に編集
			filePath = editPath(filePath);

			// 削除処理
			myConnection.deleteObject(containerNm, filePath);
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
			// パスをクラウド用に編集
			filePath = editPath(filePath);

			try {
				myConnection.describeObject(containerNm, filePath);
			}catch(NoSuchObjectException noe) {
				// ﾌｧｲﾙ(ｵﾌﾞｼﾞｪｸﾄ)が存在しない場合
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
			// パスをクラウド用に編集
			startsWith = editPath(startsWith);

			Map<QueryOption, String> map = new HashMap<QueryOption, String>();
			map.put(QueryOption.PREFIX, startsWith);

			List<Key> list = myConnection.listObjects(containerNm, map);
			for(Key key: list) {
				rtnList.add(key.getKey());
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

		Key key = null;
		try{
			// パスをクラウド用に編集
			path = editPath(path);

			// ﾌｧｲﾙｵﾌﾞｼﾞｪｸﾄの取得
			key = myConnection.describeObject(containerNm, path);

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
		rtnMap.put(FILEINFO_SIZE, String.valueOf(key.getSize()));
		// 最終更新時刻
		rtnMap.put(FILEINFO_LASTMODIFIED, sdf.format(key.getLastModified()));

		return rtnMap;
	}
}
