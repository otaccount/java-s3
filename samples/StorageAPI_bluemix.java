package org.opengion.plugin.cloud;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpSession;

import org.opengion.fukurou.util.Closer;
import org.opengion.fukurou.util.FileUtil;
import org.opengion.hayabusa.common.HybsSystemException;
import org.opengion.hayabusa.io.StorageAPI;
import org.openstack4j.api.OSClient.OSClientV3;
import org.openstack4j.api.storage.ObjectStorageService;
import org.openstack4j.model.common.DLPayload;
import org.openstack4j.model.common.Identifier;
import org.openstack4j.model.common.Payload;
import org.openstack4j.model.storage.object.SwiftObject;
import org.openstack4j.model.storage.object.options.ObjectListOptions;
import org.openstack4j.model.storage.object.options.ObjectLocation;
import org.openstack4j.openstack.OSFactory;
import org.openstack4j.openstack.internal.OSClientSession;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * bluemix用のクラウドストレージ操作実装
 * 
 * bluemix上での利用を想定しているため、ユーザ情報は環境変数VCAP_SERVICESから取得可能という前提です。
 * この環境変数はbluemix上でオブジェクトストレージを接続設定する事で自動設定されます。
 * 
 * このクラスのコンパイルには
 * openstack4j-core及び openstack4j-okhttpが必要です。
 * 実行にはそれ以外に以下のモジュールが必要です。（バージョンは作成時のもの）
 * btf-1.2.jar ,guava-20.0.jar, jackson-coreutils-1.6.jar, jackson-dataformat-yaml-2.8.8.jar, json-patch-1.9.jar, jsr305-2.0.0.jar
 *	,msg-simple-1.1.jar, okhttp-3.2.0.jar, okio-1.6.0.jar, slf4j-api-1.7.21.jar, slf4j-simple-1.7.21.jar, snakeyaml-1.15.jar
 * 
 *
 * @og.group クラウド
 * @og.rev 5.9.25.0 (2017/10/06) 新規作成
 *
 * @version 5.0
 * @author T.OTA
 * @sinse JDK7.0
 */
public class StorageAPI_bluemix implements StorageAPI {

	// クラス変数
	// ｺﾝﾃﾅ名
	String container = null;
	// ﾕｰｻﾞ名
	String username = null;
	// ﾊﾟｽﾜｰﾄﾞ
	String password = null;
	// ﾄﾞﾒｲﾝID
	String domainId = null;
	// ﾌﾟﾛｼﾞｪｸﾄID
	String projectId = null;
	// 認証URL
	String auth_url = null;

	/**
	 * ｺﾝｽﾄﾗｸﾀ
	 * bluemixに設定されているﾕｰｻﾞ情報の取得。
	 * ｼｽﾃﾑIDを名称としたｺﾝﾃﾅを作成する。
	 * @param container 
	 * @param hsession ｾｯｼｮﾝ
	 */
	public StorageAPI_bluemix(String container, HttpSession hsession) {
		// ｸﾗｽ変数に設定
		this.container = container;
		// CloudFoundryの環境変数から、接続情報を取得します。
		String env = System.getenv("VCAP_SERVICES");
		ObjectMapper mapper = new ObjectMapper();
		try {
			JsonNode node = mapper.readTree(env);
			Iterator<JsonNode> userNode = node.get("Object-Storage").elements();
			JsonNode cred = (JsonNode) userNode.next().get("credentials");

			// ﾕｰｻﾞ名
			username = cred.get("username").textValue();
			// ﾊﾟｽﾜｰﾄﾞ
			password = cred.get("password").textValue();
			// ﾄﾞﾒｲﾝID
			domainId = cred.get("domainId").textValue();
			// ﾌﾟﾛｼﾞｪｸﾄID
			projectId = cred.get("projectId").textValue();
			// 認証url
			auth_url = cred.get("auth_url").textValue() + "/v3";
		} catch (Exception e) {
			String errMsg = "VCAP_SERVICESの取得に失敗しました。ストレージと接続されているか確認して下さい。" + e;
			throw new HybsSystemException(errMsg);
		}

		// コンテナの作成(既に存在する場合は、そのまま通過する)
		try{
			ObjectStorageService objectStorage = auth(hsession);
			objectStorage.containers().create(container);
		}catch(Exception e){
			StringBuilder sbErrMsg = new StringBuilder();
			sbErrMsg.append("コンテナの作成に失敗しました。container:");
			sbErrMsg.append(container);
			sbErrMsg.append(" errInfo:");
			sbErrMsg.append(e);
			throw new HybsSystemException(sbErrMsg.toString());
		}
	}

	/**
	 * 認証処理
	 * @param hsession	ｾｯｼｮﾝ
	 * @return ObjectStorageService
	 */
	private ObjectStorageService auth(HttpSession hsession) {
		OSClientSession<?, ?> session = OSClientSession.getCurrent();
		if (session != null) {
			// 既に認証されている場合は、認証情報を返却
			return session.objectStorage();
		} else {
			// ｾｯｼｮﾝから認証ﾄｰｸﾝを取得
			String token = (String) hsession.getAttribute(SESSION_CLOUD_TOKEN);
			// 認証ﾄｰｸﾝがある場合は、ﾄｰｸﾝによる認証を行う
			if (token != null && !"".equals(token)) {
				// ﾄｰｸﾝによる認証
				OSClientV3 os = OSFactory.builderV3().endpoint(auth_url).token(token)
						.scopeToProject(Identifier.byId(projectId))
						.authenticate();
				return os.objectStorage();
			}
		}

		// ﾕｰｻﾞによる認証(ｽﾚｯﾄﾞ間はOSClientSessionに保持される)
		Identifier domainIdentifier = Identifier.byId(domainId);
		OSClientV3 os = OSFactory.builderV3().endpoint(auth_url).credentials(username, password, domainIdentifier)
				.scopeToProject(Identifier.byId(projectId))
				.authenticate();

		// 認証ﾄｰｸﾝの保持
		hsession.setAttribute(SESSION_CLOUD_TOKEN, os.getToken().getId());

		ObjectStorageService objectStorage = os.objectStorage();

		return objectStorage;
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
		// 認証
		ObjectStorageService objectStorage = auth(hsession);
		// ｱｯﾌﾟﾛｰﾄﾞｽﾄﾚｰﾑ
		Payload<InputStream> payload = new InputPayload<InputStream>(partInputStream);
		try {
			// ｱｯﾌﾟﾛｰﾄﾞ処理
			objectStorage.objects().put(this.container, updFolder + updFileName, payload);
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
			Closer.ioClose(payload);
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
		// 認証
		ObjectStorageService objectStorage = auth(hsession);
		DLPayload payload = null;
		// ﾀﾞｳﾝﾛｰﾄﾞ
		try {
			SwiftObject swiftObject = objectStorage.objects().get(ObjectLocation.create(this.container, filePath));
			payload = swiftObject.download();
		} catch (Exception e) {
			StringBuilder sbErrMsg = new StringBuilder();
			sbErrMsg.append("ストレージからのファイルダウンロードに失敗しました。filePath:");
			sbErrMsg.append(filePath);
			sbErrMsg.append(" errInfo:");
			sbErrMsg.append(e);
			throw new HybsSystemException(sbErrMsg.toString());
		}

		return payload.getInputStream();
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
		Payload<InputStream> payload = null;
		InputStream is = null;
		try {
			// openstack4jにcopyﾒｿｯﾄﾞは実装されているが、全角文字が利用できないため、
			// ﾀﾞｳﾝﾛｰﾄﾞ・ｱｯﾌﾟﾛｰﾄﾞで対応
//			objectStorage.objects().copy(ObjectLocation.create(container, oldFilePath),
//					ObjectLocation.create(container, newFilePath));
			// ｺﾋﾟｰ元情報の取得
			is = get(oldFilePath, hsession);
			// ｺﾋﾟｰ先に登録
			payload = new InputPayload<InputStream>(is);

			// 認証
			ObjectStorageService objectStorage = auth(hsession);
			objectStorage.objects().put(this.container, newFilePath, payload);
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
			Closer.ioClose(payload);
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
		// 認証
		ObjectStorageService objectStorage = auth(hsession);
		// 削除
		try {
			objectStorage.objects().delete(ObjectLocation.create(this.container, filePath));
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
	@Override
	public boolean exists(String filePath, HttpSession hsession) {
		boolean blnRtn = true;

		// 認証
		ObjectStorageService objectStorage = auth(hsession);

		try {
			SwiftObject so = objectStorage.objects().get(ObjectLocation.create(this.container, filePath));

			if (so == null) {
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
		ObjectStorageService objectStorage = auth(hsession);
		List<? extends SwiftObject> list = null;
		List<String> rtnList = new ArrayList<String>();
		try{
			// ｵﾌﾟｼｮﾝの指定
			ObjectListOptions olo = ObjectListOptions.create().startsWith(startsWith);
			// 一覧の取得
			list = objectStorage.objects().list(this.container, olo);
			for(SwiftObject so: list){
				rtnList.add(so.getName());
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
	@Override
	public Map<String, String> getInfo(String path, HttpSession hsession) {
		Map<String, String> rtnMap = new HashMap<String,String>();

		// 認証
		ObjectStorageService objectStorage = auth(hsession);

		SwiftObject so = null;
		try{
			// ﾌｧｲﾙｵﾌﾞｼﾞｪｸﾄの取得
			 so = objectStorage.objects().get(ObjectLocation.create(this.container, path));
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
		rtnMap.put(FILEINFO_SIZE, String.valueOf(so.getSizeInBytes()));
		// 最終更新時刻
		rtnMap.put(FILEINFO_LASTMODIFIED, sdf.format(so.getLastModified()));

		return rtnMap;
	}

	
	/**
	 * payloadを利用するための内部クラス
	 * (AWSやAzureで利用するか不明なので、とりあえず内部クラスとしておきます）
	 *
	 * @param <T>
	 */
	public class InputPayload<T extends InputStream> implements Payload<T>{
		private T stream = null;
	
		/**
		 * @param stream
		 */
		public InputPayload(T stream) {
			this.stream = stream;
		}
	
		@Override
		public void close() throws IOException {
			stream.close();
		}
	
		@Override
		public T open() {
			return stream;
		}
	
		@Override
		public void closeQuietly() {
			Closer.ioClose(stream);
		}
	
		@Override
		public T getRaw() {
			return stream;
		}
	}
}
