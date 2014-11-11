package fatworm.sister.cmd;


public abstract class Cmd {
	
	public static boolean isCommand(String str) {
		return str.startsWith("@");
	}
	
	public static String stripCmdName(String str){
		return str.substring(1);
	}
	
	abstract public String getName();
	
	abstract public void execute();
}
