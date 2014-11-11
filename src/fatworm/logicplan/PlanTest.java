package fatworm.logicplan;

import java.util.Scanner;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTree;

import fatworm.parser.FatwormLexer;
import fatworm.parser.FatwormParser;

public class PlanTest {
	public static void main(String[] args) throws RecognitionException {
		Scanner in = new Scanner(System.in);
		String sql = "";
		while (in.hasNextLine()) {
			String tmp = in.nextLine();
			if (tmp.startsWith(";")) break;
			else sql += tmp;
		}
		in.close();
		CommonTree t = parse(sql);
		Plan plan = LogicPlanner.translate(t);
		System.out.println(plan.toString());
	}
	
	private static CommonTree parse(String sql) throws RecognitionException {
		FatwormLexer lexer = new FatwormLexer(new ANTLRStringStream(sql));
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		FatwormParser parser = new FatwormParser(tokens);
		FatwormParser.statement_return res = parser.statement();
		System.out.println(((CommonTree)res.getTree()).toStringTree());
		return (CommonTree) res.getTree();
	}
}
