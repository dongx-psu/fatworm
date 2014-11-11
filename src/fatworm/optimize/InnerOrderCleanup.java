package fatworm.optimize;

import java.util.HashSet;
import java.util.Set;

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

public class InnerOrderCleanup {
	public static Set<String> orderField = new HashSet<String>();
	
	public static void init() {
		orderField = new HashSet<String>();
	}
	
	public static Plan process(Plan plan) {
		if (plan instanceof Distinct){
			Distinct p = (Distinct)plan;
			p.src = process(p.src);
			p.src.parent = p;
			return p;
		} else if (plan instanceof FetchTable){
			return plan;
		} else if (plan instanceof Group) {
			Group p = (Group)plan;
			p.src = process(p.src);
			p.src.parent = p;
			return p;
		} else if (plan instanceof Product) {
			Product p = (Product)plan;
			p.left = process(p.left);
			p.right = process(p.right);
			p.left.parent = p;
			p.right.parent = p;
			return p;
		} else if (plan instanceof None) {
			return plan;
		} else if (plan instanceof ConstPlan) {
			return plan;
		} else if (plan instanceof Order) {
			Order p = (Order)plan;
			boolean flag = !orderField.isEmpty();
			for (String x:p.orderField)
				if (!orderField.contains(Util.getAttr(x).toLowerCase()))
					orderField.add(Util.getAttr(x).toLowerCase());

			p.src = process(p.src);
			p.src.parent = flag ? p.parent : p;
			return flag ? p.src : p;
		} else if (plan instanceof Project) {
			Project p = (Project)plan;
			p.src = process(p.src);
			p.src.parent = p;
			return p;
		} else if (plan instanceof Rename) {
			Rename p = (Rename)plan;
			p.src = process(p.src);
			p.src.parent = p;
			return p;
		} else if (plan instanceof TableRename) {
			TableRename p = (TableRename)plan;
			p.src = process(p.src);
			p.src.parent = p;
			return p;
		} else if (plan instanceof Select) {
			Select p = (Select)plan;
			p.src = process(p.src);
			p.src.parent = p;
			return p;
		} else if (plan instanceof Join) {
			Join p = (Join)plan;
			p.left = process(p.left);
			p.right = process(p.right);
			p.left.parent = p;
			p.right.parent = p;
			return p;
		} else {
			Util.warn("ClearInnerOrders:meow!!!");
		}
		return null;
	}
}
