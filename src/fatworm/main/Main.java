package fatworm.main;

import java.util.Scanner;

import fatworm.driver.DBEngine;
import fatworm.logicplan.Plan;

public class Main {
	public Main() {
		// TODO Auto-generated constructor stub
	}
	
	public static void main(String[] args) throws Throwable {
		DBEngine db = DBEngine.getInstance();
		Scanner in = new Scanner(System.in);
		System.out.print(">");
		db.open("d:/database/fatworm");
		while (in.hasNextLine()) {
				try {
					String tmp = in.nextLine();
					if(tmp.equalsIgnoreCase("quit")||tmp.equalsIgnoreCase("exit")||tmp.equalsIgnoreCase("\\q"))
						break;
					fatworm.driver.ResultSet result = db.execute(tmp);
					Plan plan = result.plan;
					if(!plan.hasNext()) 
						System.out.println("no results");
					while(plan.hasNext()){
						System.out.println(plan.next().toString());
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				System.out.print("\r\n>");
		}
		in.close();
		db.close();
	}
}
