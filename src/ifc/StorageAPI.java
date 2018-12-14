package ifc;

import java.io.InputStream;
import java.util.Map;

/**
 * クラウドストレージ操作用のインターフェイス。
 * 
 * 継承クラスのコンストラクターはコンテナ名とHTTPセッションを持たせます。
 *
 * @og.group
 * @og.rev 5.9.25.0 (2017/10/06) 新規作成
 *
 * @version 5.0
 * @author T.OTA
 * @sinse JDK7.0
 */
public interface StorageAPI {
	/**
	 * トークンキー
	 */
	public static final String SESSION_CLOUD_TOKEN = "SESSION_CLOUD_TOKEN";
	
	/**
	 *  ファイル情報に格納されている値
	 *  サイズ
	 */
	public static final String FILEINFO_SIZE = "SIZE";

	/**
	 * 最終更新時刻
	 */
	public static final String FILEINFO_LASTMODIFIED = "LASTMODIFIED";

	/**
	 * 削除
	 *
	 * @param filePath	削除ﾌｧｲﾙのﾊﾟｽ
	 * @param hsession	ｾｯｼｮﾝ
	 */
	void delete(String filePath);

	/**
	 * ｺﾋﾟｰ
	 *
	 * @param oldFilePath	ｺﾋﾟｰ元ﾌｧｲﾙﾊﾟｽ
	 * @param newFilePath	ｺﾋﾟｰ先ﾌｧｲﾙﾊﾟｽ
	 * @param hsession		ｾｯｼｮﾝ
	 */
	void copy(String oldFilePath, String newFilePath);

	/**
	 * ﾀﾞｳﾝﾛｰﾄﾞ
	 *
	 * @param filePath	ﾀﾞｳﾝﾛｰﾄﾞ対象のﾌｧｲﾙﾊﾟｽ
	 * @param hsession	ｾｯｼｮﾝ
	 * @return ストリーム
	 */
	InputStream get(String filePath);

	/**
	 * ｱｯﾌﾟﾛｰﾄﾞ
	 *
	 * @param partInputStream	ｱｯﾌﾟﾛｰﾄﾞ対象のｽﾄﾘｰﾑ
	 * @param updFolder		ｱｯﾌﾟﾛｰﾄﾞﾌｫﾙﾀ名
	 * @param updFileName		ｱｯﾌﾟﾛｰﾄﾞﾌｧｲﾙ名
	 * @param hsession			ｾｯｼｮﾝ
	 */
	void add(InputStream partInputStream, String updFolder, String updFileName);

	/**
	 * ﾌｧｲﾙ名変更
	 *
	 * @param fileUrl		ﾌｧｲﾙﾊﾟｽ
	 * @param oldFileName	変更前ﾌｧｲﾙ名
	 * @param newFileName	変更後ﾌｧｲﾙ名
	 * @param useBackup	変更後ﾌｧｲﾙ名が既に存在する場合のﾊﾞｯｸｱｯﾌﾟ作成ﾌﾗｸﾞ
	 * @param session		ｾｯｼｮﾝ
	 */
	void rename(String fileUrl, String oldFileName, String newFileName, final boolean useBackup);

	/**
	 * ﾌｧｲﾙ一覧取得
	 *
	 * @param startsWith	ﾊﾟｽの前方一致
	 * @param hsession		ｾｯｼｮﾝ
	 * @return				ﾌｧｲﾙﾊﾟｽ一覧
	 */
	String[] list(String startsWith);

	/**
	 * ﾌｧｲﾙ存在ﾁｪｯｸ
	 *
	 * @param path			ﾌｧｲﾙﾊﾟｽ
	 * @param hsession		ｾｯｼｮﾝ
	 * @return				true:存在 false:存在しない
	 */
	boolean exists(String path);

	/**
	 * ﾌｧｲﾙ情報取得
	 *
	 * @param path			ﾌｧｲﾙﾊﾟｽ
	 * @param hsession		ｾｯｼｮﾝ
	 * @return				ﾌｧｲﾙ情報格納Map
	 */
	Map<String,String> getInfo(String path);
}
