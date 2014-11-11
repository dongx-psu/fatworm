package fatworm.optimize;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import fatworm.expr.BinaryExpr;
import fatworm.expr.BinaryOp;
import fatworm.expr.Expr;
import fatworm.expr.ID;
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

public class SelectPusher {
	public static Plan process(Plan plan) {
		plan = decomposeAnd(plan);
		plan = pushSelect(plan);
		return plan;
	}
	
	public static Plan pushSelect(Plan plan) {
		if (plan instanceof Distinct) {
			Distinct p = (Distinct)plan;
			p.src = pushSelect(p.src);
			p.src.parent = p;
			return p;
		} else if (plan instanceof FetchTable) {
			return plan;
		} else if (plan instanceof Group) {
			Group p = (Group)plan;
			p.src = pushSelect(p.src);
			p.src.parent = p;
			return p;
		} else if (plan instanceof Product) {
			Product p = (Product)plan;
			p.left = pushSelect(p.left);
			p.right = pushSelect(p.right);
			p.left.parent = p;
			p.right.parent = p;
			return p;
		} else if (plan instanceof None) {
			return plan;
		} else if (plan instanceof ConstPlan) {
			return plan;
		} else if (plan instanceof Order) {
			Order p = (Order)plan;
			p.src = pushSelect(p.src);
			p.src.parent = p;
			return p;
		} else if (plan instanceof Project) {
			Project p = (Project)plan;
			p.src = pushSelect(p.src);
			p.src.parent = p;
			return p;
		} else if (plan instanceof Rename) {
			Rename p = (Rename)plan;
			p.src = pushSelect(p.src);
			p.src.parent = p;
			return p;
		} else if (plan instanceof TableRename) {
			TableRename p = (TableRename)plan;
			p.src = pushSelect(p.src);
			p.src.parent = p;
			return p;
		} else if (plan instanceof Select) {
			Select p = (Select)plan;
			if (p.hasPushed) {
				p.src = pushSelect(p.src);
				p.src.parent = p;
				return p;
			}
			
			Plan head = p;
			Plan child = p.src;
			Select current = p;
			while (current.canPush()) {
				if (current.src instanceof TableRename) {
					TableRename curChild = (TableRename) current.src;
					String tbl = curChild.alias;
					for (String newName : curChild.src.getColumns()) {
						String oldName = tbl + "." + Util.getAttr(newName);
						current.pred.rename(oldName, newName);
					}
				} else if (current.src instanceof Rename) {
					Rename curChild = (Rename) current.src;
					for (int i = 0; i < curChild.as.size(); i++) {
						String oldName = curChild.as.get(i);
						String newName = curChild.src.getColumns().get(i);
						current.pred.rename(oldName, newName);
					}
				}
				push(current);
				if (head == p) head = child;
			}
			
			current.hasPushed = true;
			head = pushSelect(head);
			return head;
		} else if (plan instanceof Join) {
			Join p = (Join)plan;
			p.left = pushSelect(p.left);
			p.right = pushSelect(p.right);
			p.left.parent = p;
			p.right.parent = p;
			return p;
		} else {
			Util.warn("Err in Push select");
		}
		return plan;
	}

	public static void push(Select select) {
		Plan parent = select.parent;
		Plan child = select.src;
		if (parent != null)
			parent.setSrc(select, child);
		else child.parent = null;
		
		if (child instanceof Distinct) {
			Distinct p = (Distinct)child;
			select.setSrc(child, p.src);
			child.setSrc(p.src, select);
		} else if (child instanceof Product) {
			Product p = (Product)child;
			if (Util.subsetof(select.pred.getRequestedColumns(), p.left.getColumns())) {
				select.setSrc(select.src, p.left);
				child.setSrc(p.left, select);
			} else if(Util.subsetof(select.pred.getRequestedColumns(), p.right.getColumns())) {
				select.setSrc(select.src, p.right);
				child.setSrc(p.right, select);
			}
		} else if (child instanceof Order) {
			Order p = (Order)child;
			select.setSrc(child, p.src);
			child.setSrc(p.src, select);
		} else if (child instanceof Project) {
			Project p = (Project)child;
			select.setSrc(child, p.src);
			child.setSrc(p.src, select);
		} else if (child instanceof Rename) {
			Rename p = (Rename)child;
			select.setSrc(child, p.src);
			child.setSrc(p.src, select);
		} else if (child instanceof TableRename) {
			TableRename p = (TableRename)child;
			select.setSrc(child, p.src);
			child.setSrc(p.src, select);
		} else if (child instanceof Select) {
			Select p = (Select)child;
			select.setSrc(child, p.src);
			child.setSrc(p.src, select);
		} else {
			Util.warn("err in select pushing");
		}
	}

	public static Plan decomposeAnd(Plan plan) {
		if (plan instanceof Distinct) {
			Distinct p = (Distinct)plan;
			p.src = decomposeAnd(p.src);
			p.src.parent = p;
			return p;
		} else if (plan instanceof FetchTable) {
			return plan;
		} else if (plan instanceof Group) {
			Group p = (Group)plan;
			p.src = decomposeAnd(p.src);
			p.src.parent = p;
			return p;
		} else if (plan instanceof Product) {
			Product p = (Product)plan;
			p.left = decomposeAnd(p.left);
			p.right = decomposeAnd(p.right);
			p.left.parent = p;
			p.right.parent = p;
			return p;
		} else if (plan instanceof None) {
			return plan;
		} else if (plan instanceof ConstPlan) {
			return plan;
		} else if (plan instanceof Order) {
			Order p = (Order)plan;
			p.src = decomposeAnd(p.src);
			p.src.parent = p;
			return p;
		} else if (plan instanceof Project) {
			Project p = (Project)plan;
			p.src = decomposeAnd(p.src);
			p.src.parent = p;
			return p;
		} else if (plan instanceof Rename) {
			Rename p = (Rename)plan;
			p.src = decomposeAnd(p.src);
			p.src.parent = p;
			return p;
		} else if (plan instanceof TableRename) {
			TableRename p = (TableRename)plan;
			p.src = decomposeAnd(p.src);
			p.src.parent = p;
			return p;
		} else if (plan instanceof Select) {
			Select p = (Select)plan;
			p.src = decomposeAnd(p.src);
			p.src.parent = p;
			if (!p.pred.isAnd())
				return p;
			
			List<Expr> cond = new LinkedList<Expr>();
			p.pred.collectCondition(cond);
			boolean flag = false;
			while (!flag) {
				Set<Expr> toAdd = new HashSet<Expr>();
				for (int i = 0; i < cond.size(); i++) {
					Expr e = cond.get(i);
					if(!(e instanceof BinaryExpr) || ((BinaryExpr)e).op!=BinaryOp.EQ)
						continue;
					BinaryExpr x = (BinaryExpr) e;
					ID x1 = (x.l instanceof ID) ? (ID)x.l:null;
					ID x2 = (x.r instanceof ID) ? (ID)x.r:null;
					if (x1 == null && x2 == null) continue;
					for (int j = i + 1; j < cond.size(); j++){
						Expr ee = cond.get(j);
						if (!(ee instanceof BinaryExpr) || ((BinaryExpr)ee).op != BinaryOp.EQ)
							continue;
						BinaryExpr y = (BinaryExpr) ee;
						ID y1 = (y.l instanceof ID) ? (ID)y.l : null;
						ID y2 = (y.r instanceof ID) ? (ID)y.r : null;
						if (y1 == null && y2 == null) continue;
						if (x1 != null && y1 != null && x1.name.equalsIgnoreCase(y1.name)) {
							toAdd.add(new BinaryExpr(x1, BinaryOp.EQ, y.r));
							toAdd.add(new BinaryExpr(y1, BinaryOp.EQ, x.r));
							toAdd.add(new BinaryExpr(y.r, BinaryOp.EQ, x.r));
						} else if(x2 != null && y1 != null && x2.name.equalsIgnoreCase(y1.name)) {
							toAdd.add(new BinaryExpr(x2, BinaryOp.EQ, y.r));
							toAdd.add(new BinaryExpr(y1, BinaryOp.EQ, x.l));
							toAdd.add(new BinaryExpr(y.r, BinaryOp.EQ, x.l));
						} else if(x1 != null && y2 != null && x1.name.equalsIgnoreCase(y2.name)) {
							toAdd.add(new BinaryExpr(x1, BinaryOp.EQ, y.l));
							toAdd.add(new BinaryExpr(y2, BinaryOp.EQ, x.r));
							toAdd.add(new BinaryExpr(y.l, BinaryOp.EQ, x.r));
						} else if (x2 != null && y2 != null && x2.name.equalsIgnoreCase(y2.name)){
							toAdd.add(new BinaryExpr(x2, BinaryOp.EQ, y.l));
							toAdd.add(new BinaryExpr(y2, BinaryOp.EQ, x.l));
							toAdd.add(new BinaryExpr(y.l, BinaryOp.EQ, x.l));
						}
					}
				}
				toAdd.addAll(cond);
				flag = cond.size() >= toAdd.size();
				cond = new LinkedList<Expr>(toAdd);
			}
			
			Plan ans = p.src;
			for (Expr e : cond)
				ans = new Select(ans, e);
			
			ans.parent = p.parent;
			return ans;
		} else if (plan instanceof Join) {
			Join p = (Join)plan;
			p.left = decomposeAnd(p.left);
			p.right = decomposeAnd(p.right);
			p.left.parent = p;
			p.right.parent = p;
			return p;
		} else {
			Util.warn("Err in decomposing and");
		}
		return null;
	}
}
