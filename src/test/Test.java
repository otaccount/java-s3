package test;

import cls.Prop;
import lombok.Getter;
import lombok.val;
import ot.utils.Basic;

public class Test extends Basic {

	public static void main(String[] args) {
		// TODO 自動生成されたメソッド・スタブ
		out("start");

		test01();

		out("end");
	}

	public static void test01() {
		try {
			val a = new Prop();
			out(a.get("key"));
		}catch(Exception e) {
			out(e);
		}
	}
}

@Getter
class Sample {
	private String str;

	public Sample(String str) {
		this.str = str;
	}

	public String test() {
		return "test" + str;
	}
}