package test;

import static ot.utils.Basic.*;

import cls.File;

public class FileTest {

	public static void main(String[] args) {
		// TODO 自動生成されたメソッド・スタブ
		test01();
	}

	public static void test01() {
		out("start");
		
		String path="test.txt";
		
		File file = new File(path);
		
		out(file.exists());
		
		out("end");
	}
}
