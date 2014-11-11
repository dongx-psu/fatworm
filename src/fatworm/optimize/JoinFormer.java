package fatworm.optimize;

import fatworm.logicplan.ConstPlan;
import fatworm.logicplan.Distinct;
import fatworm.logicplan.FetchTable;
import fatworm.logicplan.Group;
import fatworm.logicplan.Join;
import fatworm.logicplan.None;
import fatworm.logicplan.Order;
import fatworm.logicplan.Plan;
import fatworm.logicplan.Product;
import fatworm.logicplan.Project;
import fatworm.logicplan.Rename;
import fatworm.logicplan.Select;
import fatworm.logicplan.TableRename;
import fatworm.util.Util;

public class JoinFormer {
	public static void process(Plan plan) {
		if (plan instanceof Distinct) {
			Distinct p = (Distinct)plan;
			process(p.src);
		} else if (plan instanceof FetchTable) {
		} else if (plan instanceof Group) {
			Group p = (Group)plan;
			process(p.src);
		} else if (plan instanceof Product) {
			Product p = (Product)plan;
			process(p.left);
			process(p.right);
			
			Plan parent = p.parent;
			Plan current = p;
			if (!(parent instanceof Select)) return;
			Join ans = new Join(p);
			while (parent instanceof Select) {
				ans.addCond(((Select)parent).pred);
				current = parent;
				parent = parent.parent;
			}
			
			if (parent != null)
				parent.setSrc(current, ans);
			ans.parent = parent;
		} else if (plan instanceof None) {
		} else if (plan instanceof ConstPlan) {
		} else if (plan instanceof Order) {
			Order p = (Order)plan;
			process(p.src);
		} else if (plan instanceof Project) {
			Project p = (Project)plan;
			process(p.src);
		} else if (plan instanceof Rename) {
			Rename p = (Rename)plan;
			process(p.src);
		} else if (plan instanceof TableRename) {
			TableRename p = (TableRename)plan;
			process(p.src);
		} else if (plan instanceof Select) {
			Select p = (Select)plan;
			process(p.src);
		} else if (plan instanceof Join) {
			Join p = (Join)plan;
			process(p.left);
			process(p.right);
		} else {
			Util.warn("transformTheta:meow!!!");
		}
	}
}
