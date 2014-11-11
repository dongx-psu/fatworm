package fatworm.sister.cmd;

import fatworm.sister.Sister;

public class TestCaseCmd extends Cmd {

	private Sister sis;
	
	public TestCaseCmd(Sister sis) {
		this.sis = sis;
	}
	
	@Override
	public String getName() {
		return "TEST_CASE";
	}

	@Override
	public void execute() {
		String testCase = sis.nextLine();
		// Eat the next ";" line
		sis.nextLine();
		sis.includeNewFile(testCase + ".fwt");
	}

}
