package cls;

import ifc.StorageAPI;

public class File {
	private String path;
	private java.io.File localFile;
	private StorageAPI storage;
	private boolean isLocal = true;
	
	
	public File(String path) {
		if(isLocal) {
			// ローカルの場合
			localFile = new java.io.File("");
		}else {
			// クラウドの場合
			this.path = path;
//			storage = new [クラウド判定]
		}
	}
	
	public boolean exists() {
		if(isLocal) {
			// ローカルの場合
			return localFile.exists();
		}else {
			// クラウドの場合
			return storage.exists(this.path);
		}
	}
}
