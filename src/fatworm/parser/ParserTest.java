package fatworm.parser;

import java.util.Scanner;

import org.antlr.runtime.*;
import org.antlr.runtime.tree.CommonTree;

public class ParserTest {
	public static void main(String[] args) throws RecognitionException {
		Scanner in = new Scanner(System.in);
		while (in.hasNextLine()) {
			parse(in.nextLine());
		}
		in.close();
	}
	
	private static void parse(String sql) throws RecognitionException {
		FatwormLexer lexer = new FatwormLexer(new ANTLRStringStream(sql));
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		FatwormParser parser = new FatwormParser(tokens);
		FatwormParser.statement_return res = parser.statement();
		CommonTree t = (CommonTree) res.getTree();
		System.out.println(t.toStringTree());
	}
}
