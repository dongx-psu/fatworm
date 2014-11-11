package fatworm.sister;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import fatworm.sister.cmd.TestCaseCmd;

public class Launcher {

	private static String testName;

	private static String url = "jdbc:fatworm:/"
			+ System.getProperty("user.dir") + File.separator + "db";

	private static String outputFile = null;

	public static void main(String[] args) {
		init(args);
		launch();
	}

	private static void init(String[] args) {
		GnuParser parser = new GnuParser();
		try {
			CommandLine cl = parser.parse(getOptions(), args);
			parseArguments(cl);
		} catch (ParseException e) {
			printUsage();
		}
	}

	private static void parseArguments(CommandLine cl) {
		if (cl.hasOption("h")) {
			printUsage();
			return;
		}

		if (!cl.hasOption("n")) {
			System.out.println("Please Specify The Name of Test Case");
			printUsage();
			return;
		}

		testName = cl.getOptionValue("n");

		if (cl.hasOption("o"))
			outputFile = cl.getOptionValue("o");
	}

	private static Options getOptions() {
		Options ops = new Options();

		ops.addOption("n", "name", true, "The name of test suit");

		ops.addOption("o", "output", true,
				"The path of file to dump all output");

		ops.addOption("h", "help", false, "Print Usage");

		return ops;
	}

	private static void printUsage() {
		HelpFormatter hf = new HelpFormatter();
		hf.printHelp("Fatworm Your Sister", getOptions());
	}

	private static void launch() {
		SqlClient client = new SqlClient();
		client.connect(url);
		if (outputFile != null)
			client.setOutput(outputFile);

		Sister sis = new Sister(testName, client);
		registerCommands(sis);
		sis.start();
		client.disconnect();
	}

	private static void registerCommands(Sister sis) {
		sis.registryCmd(new TestCaseCmd(sis));
	}
}
