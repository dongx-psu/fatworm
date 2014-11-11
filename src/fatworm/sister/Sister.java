package fatworm.sister;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Stack;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import fatworm.sister.cmd.Cmd;

public final class Sister {
	
	public static final String TESTSUIT_CONFIG_NAME = "testsuit.conf";
	
	private static Logger logger = Logger.getLogger(Sister.class);
	
	private final String testName;
	
	private final Stack<BufferedReader> fileStack;
	
	private final Map<String, Cmd> cmdMap;
	
	public final SqlClient client;
	
	public Sister(String testName, SqlClient client) {
		this.testName = testName;
		this.fileStack = new Stack<BufferedReader>();
		this.cmdMap = new TreeMap<String, Cmd>();
		this.client = client;
	}

	public void registryCmd(Cmd cmd) {
		cmdMap.put(cmd.getName(), cmd);
	}
	
	public void start() {
		includeNewFile(TESTSUIT_CONFIG_NAME);
		mainLoop();
	}
	
	private void mainLoop() {
		while (!fileStack.isEmpty()) {
			String line = nextLine();
			if (line == null)
				continue;
			
			if (Cmd.isCommand(line)) {
				String cmdName = Cmd.stripCmdName(line);
				Cmd cmd = cmdMap.get(cmdName);
				if (cmd == null) {
					logger.error("Cannot find the command handler for " + cmdName);
					continue;
				}
				cmd.execute();
				continue;
			}
			
			client.appendLine(line);
		}
	}
	
	public void includeNewFile(String file) {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(testName + File.separator + file));
			fileStack.push(reader);
		} catch (FileNotFoundException e) {
			logger.error("Test File Not Found: " + file);
		}
	}
	
	public String nextLine() {
		BufferedReader reader = fileStack.peek();
		String ret = null;
		try {
			ret = reader.readLine();
		} catch (IOException e) {
			logger.error(e);
			return null;
		}
		
		if (ret == null)
			fileStack.pop();
	
		return ret;
	}
}
