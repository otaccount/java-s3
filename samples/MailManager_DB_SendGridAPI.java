package org.opengion.plugin.cloud;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opengion.fukurou.db.DBUtil;
import org.opengion.hayabusa.common.HybsSystem;
import org.opengion.hayabusa.common.HybsSystemException;
import org.opengion.hayabusa.mail.MailManager_DB;
import org.opengion.hayabusa.mail.MailPattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sendgrid.Method;
import com.sendgrid.Request;
import com.sendgrid.SendGrid;

/**
 * パッチによるメール送信の実装クラスです。
 * 送信デーモンはパラメータテーブル(GE30)を監視して、新規のデータが登録されたら、
 * そのデータをパラメータとしてメール合成処理メソッドに渡して合成を行って送信します。
 * 最後に、処理結果を受取って、パラメータテーブルの状況フラグを送信済/送信エラーに更新します。
 * エラーが発生した場合、エラーテーブルにエラーメッセージを書き込みます。
 * 
 * hayabusa.mailの標準クラスを継承して作成しています。
 * 基本的な動作は同じですが、メール送信にSMTPではなくsendGridのAPIを利用します。
 * MAIL_SENDGRID_APIKEYをシステムリソースとして登録する必要があります。
 * 
 * 一時的に利用できなくなる事を想定して、
 * 一定時間の間（ハードコーディングで10分としている）はエラーが発生しても再送を試みるようにします。
 * 
 * このクラスをコンパイルするためにはsendgrid-java-4.1.1.jar,java-http-client-4.1.0.jarが必要です。
 * 実行にはhamcrest-core-1.1.jar,httpclient-4.5.2.jar,httpcore-4.4.4.jar,mockito-core-1.10.19.jar,objenesis-2.1.jar
 * ,jackson-annotations-2.5.3.jar,jackson-core-2.5.3.jar,jackson-databind-2.5.3.jarが必要です。
 *
 * @og.group	メールモジュール
 *
 * @og.rev 5.9.26.0 (2017/11/02) 新規作成
 * @author		T.OTA
 * @sinse		JDK1.7
 *
 */
public class MailManager_DB_SendGridAPI extends MailManager_DB {
	private static final String	selGE30DYSET	= "SELECT DYSET FROM GE30 WHERE UNIQ = ?";	// 2017/10/27 ADD 登録時刻の取得
	// SendGridのAPIキー
	private static final String SENDGRID_APIKEY = HybsSystem.sys("MAIL_SENDGRID_APIKEY");
	// メール送信先のtoリスト
	private ArrayList<String> toList = new ArrayList<String>();
	// メール送信先のccリスト
	private ArrayList<String> ccList = new ArrayList<String>();
	// メール送信先のbccリスト
	private ArrayList<String> bccList = new ArrayList<String>();

	/**
	 * バッチより呼出のメインメソッドです。
	 * パラメータテーブル(GE30)を監視します。
	 * 新規のデータが登録されたら、メール文を合成して送信を行います。
	 * エラーが発生した場合、エラーテーブルにエラーメッセージを書き込みます。
	 *
	 * @param systemId システムID
	 */
	@Override
	public void sendDBMail( final String systemId ){
		// パラメータテーブルよりバッチでセットしたデータを取得します。
		String[][] ge30datas = DBUtil.dbExecute( selGE30, new String[]{ systemId, HybsSystem.getDate( "yyyyMMddHHmmss" ) }, appInfo, DBID );	// 5.9.18.0 (2017/03/02)

		// 2017/10/27 ADD SendGrid利用の追加対応
		String timePre1Hour = "";
		// タイムスタンプの設定
		timePre1Hour = getTimePre1Hour();

		int ge30Len = ge30datas.length;

		for( int i=0; i < ge30Len; i++ ) {
			String fgj = SNED_OK;
			try {
				Map<String, String> initParam = makeParamMap( systemId, ge30datas[i] );
				create( initParam );
				send();								// 合成されたメール文書、宛先で送信処理を行います。
				errMsgList.addAll( getErrList() );
			}
			catch( RuntimeException rex ) {
				fgj = SNED_NG;
				errMsgList.add( "メール送信失敗しました。パラメータキー：" + ge30datas[i][GE30_UNIQ] + " " + rex.getMessage() );
			}
			finally {
				if(fgj != SNED_NG){
					commitParamTable( ge30datas[i][GE30_UNIQ], fgj );
				}else{
					// エラーレコードの登録日時を取得
					String[][] rec = DBUtil.dbExecute( selGE30DYSET, new String[]{ge30datas[i][GE30_UNIQ]}, appInfo, DBID);
					String DYSET = rec[0][0];

					if(DYSET.compareTo(timePre1Hour) < 0){
						// 登録から一定時間以上のエラーをエラーに更新
						commitParamTable( ge30datas[i][GE30_UNIQ], fgj );
					}
					else {
						// それ以外は再送を試みる
						commitParamTable( ge30datas[i][GE30_UNIQ], "1" );
						
					}
				}

				if ( ! errMsgList.isEmpty() ) {
					writeErrorTable( ge30datas[i][GE30_UNIQ], systemId, errMsgList );
					errMsgList.clear();
				}
			}
		}
	}

	/**
	 * １時間前のタイムスタンプを取得
	 *
	 * @return タイムスタンプ(１時間前)
	 */
	private String getTimePre1Hour(){
		Date date = new Date();
		Calendar call = Calendar.getInstance();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		call.setTime(date);
		// sendGridが一時的に使えなくなる場合を考慮
		// 10分間は再送を試みる
		call.add(Calendar.MINUTE, -10);

		return sdf.format(call.getTime());
	}

	/**
	 * SendGridApiを利用して、メール送信を行うメソッドです。
	 *
	 */
	@Override
	public void send(){
		// 宛先
		List<String> invalidAddrBuf	= new ArrayList<String>();
		setMailDst(invalidAddrBuf);

		try{
			SendGrid sg = new SendGrid(SENDGRID_APIKEY);

			Request request = new Request();
			request.setMethod(Method.POST);
			request.setEndpoint("mail/send");

			// SengGrid向けJsonの設定
			request.setBody(makeJson());

			// メール送信要求
			sg.api(request);

			// 送信結果を履歴テーブル、宛先テーブルにセットします。
			commitMailDB();

		}catch(IOException e){
			String errMsg = "送信時にエラー発生しました。" + e.getMessage();
			throw new RuntimeException( errMsg,e );
		}
	}

	/**
	 * SendGrid向けのJsonを生成します。
	 * @return JSONデータ
	 */
	private String makeJson(){
		String rtnJson = "";
		Map<Object,Object> jsonMap = new HashMap<Object, Object>();
		// 送信先の設定
		Map<String,List<Map<String,String>>> sendMap = new HashMap<String,List<Map<String,String>>>();
		sendMap.put("to", setSendList(toList));
		if(!ccList.isEmpty()){
			sendMap.put("cc", setSendList(ccList));
		}
		if(!bccList.isEmpty()){
			sendMap.put("bcc",  setSendList(bccList));
		}
		jsonMap.put("personalizations", new Map[]{sendMap});
		// タイトル
		jsonMap.put("subject",getTitle());
		// 送信元
		jsonMap.put("from", setMap("email",getFromAddr()));
		// 内容
		Map<String,String> contentMap = new HashMap<String,String>();
		contentMap.put("type","text/plain");
		contentMap.put("value",getContent());
		jsonMap.put("content", new Map[]{contentMap});

		ObjectMapper mapper = new ObjectMapper();

		try{
			rtnJson = mapper.writeValueAsString(jsonMap);
		}catch(JsonProcessingException e){
			String errMsg = "JSONの生成に失敗しました。" + e;
			throw new HybsSystemException(errMsg);
		}

		return rtnJson;
	}

	/**
	 * Map格納用メソッド
	 *
	 * @param val1
	 * @param val2
	 * @return マップ
	 */
	private Map<Object,Object> setMap(Object val1, Object val2){
		Map<Object,Object> rtnMap = new HashMap<Object,Object>();
		rtnMap.put(val1,val2);
		return rtnMap;
	}

	/**
	 * メール送信先リストをJSON用リストに設定
	 *
	 * @param list
	 * @return JSON用リスト
	 */
	private List<Map<String,String>> setSendList(ArrayList<String> list){
		// toリスト
		List<Map<String,String>> rtnList = new ArrayList<Map<String,String>>();
		for(String str: list){
			Map<String,String> map = new HashMap<String,String>();
			map.put("email", str);
			rtnList.add(map);
		}
		return rtnList;
	}

	/**
	 * 宛先マップを元に、送信オブジェクトに宛先をセットします。
	 * セットする際に、アカウントエラーとなっているアドレスを除外します。
	 * 宛先が存在しない場合、例外を投げます。
	 *
	 * 計算方法は親クラスのprivateメソッドを流用。
	 * 値はクラス変数のリストに格納するように変更しています。
	 *
	 * @param invalidAddr 宛先のリスト
	 */
	private void setMailDst( final List<String> invalidAddr ){

		Map<Integer, ArrayList<String>> tempMap = new HashMap<Integer, ArrayList<String>>();
		tempMap.put( Integer.valueOf( MailPattern.KBN_TO ),  toList );
		tempMap.put( Integer.valueOf( MailPattern.KBN_CC ),  ccList );
		tempMap.put( Integer.valueOf( MailPattern.KBN_BCC ), bccList );

		Map tmp = getMailDstMap();
		for( String dstId : getMailDstMap().keySet()) {
			String[] dstInfo = getMailDstMap().get( dstId );
			Integer kbn = Integer.valueOf( dstInfo[MailPattern.IDX_DST_KBN] );
			if( !invalidAddr.contains( dstInfo[MailPattern.IDX_DST_ADDR] )
					&& !FGJ_ADDR_ERR.equals( dstInfo[MailPattern.IDX_FGJ] )){
				dstInfo[MailPattern.IDX_FGJ] = FGJ_SEND_OVER;

				String name = dstInfo[MailPattern.IDX_DST_NAME];
				if( name != null && name.length() > 0 ) {
					tempMap.get( kbn ).add( dstInfo[MailPattern.IDX_DST_NAME] +  "<"+ dstInfo[MailPattern.IDX_DST_ADDR] + ">" );
				}
				else {
					tempMap.get( kbn ).add( dstInfo[MailPattern.IDX_DST_ADDR] );
				}
			}
			else {
				if( FGJ_SEND_OVER.equals( dstInfo[MailPattern.IDX_FGJ] ) ) {
					dstInfo[MailPattern.IDX_FGJ] = FGJ_ACNT_ERR;
				}
			}
		}

		// 宛先が全部無効の場合、例外を投げます
		if( toList.isEmpty() && ccList.isEmpty() && bccList.isEmpty()){
			String errMsg = "宛先のメールアドレスが有効ではありません。"
					+ "TO , CC , BCC のいづれにもアドレスが設定されていません。";
			throw new RuntimeException( errMsg );
		}
	}

	/**
	 * エラーテーブルにエラーメッセージを登録します。
	 * 親のprivateメソッドを流用。エラーメールの送信は行いません。
	 *
	 * @param	paraKey		パラメータキー(GE36.PARA_KEY)
	 * @param	systemId	システムID
	 * @param	emList		エラーメッセージリスト
	 *
	 */
	private void writeErrorTable( final String paraKey, final String systemId, final List<String> emList ){
		String[] insGE36Args = new String[6];
		insGE36Args[GE36_PARA_KEY]	= paraKey;
		insGE36Args[GE36_DYSET] 	= HybsSystem.getDate( "yyyyMMddHHmmss" );
		insGE36Args[GE36_USRSET] 	= "DAEMON";
		insGE36Args[GE36_PGUPD] 	= "DAEMON";
		insGE36Args[GE36_SYSTEM_ID] = systemId;
		for( int i=0; i< emList.size(); i++ ){
			insGE36Args[GE36_ERRMSG] = trim( emList.get( i ), 4000);
			DBUtil.dbExecute( insGE36, insGE36Args, appInfo, DBID );
		}
	}
}
