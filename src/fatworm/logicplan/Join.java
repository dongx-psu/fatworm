package fatworm.logicplan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import fatworm.expr.BinaryExpr;
import fatworm.expr.BinaryOp;
import fatworm.expr.Expr;
import fatworm.expr.ID;
import fatworm.table.Record;
import fatworm.table.Schema;
import fatworm.type.Type;
import fatworm.util.Env;
import fatworm.util.Util;

public class Join extends Plan {
	public Plan left;
	public Plan right;
	public List<Expr> condList;
	public Record curLeft;
	public Record current;
	public Schema schema;
	private Env env;
	private boolean canMergeJoin = false;
	private String leftName;
	private String rightName;
	private List<Record> results = new ArrayList<Record>();
	private int ptr = 0;
	
	public Join(Product p) {
		left = p.left;
		right = p.right;
		left.parent = this;
		right.parent = this;
		condList = new LinkedList<Expr>();
		curLeft = null;
		myAggr = p.getAggr();
		schema = p.getSchema();
	}

	@Override
	public void eval(Env envGlobal) {
		hasEval = true;
		curLeft = null;
		this.env = envGlobal;
		BinaryExpr cond = checkMergeJoin();
		if (cond != null){
			canMergeJoin = true;
			if (left.getSchema().findStrictIndex(cond.l.toString())>=0 && right.getSchema().findStrictIndex(cond.r.toString())>=0) {
				leftName = cond.l.toString();
				rightName = cond.r.toString();
			} else if (right.getSchema().findStrictIndex(cond.l.toString())>=0 && left.getSchema().findStrictIndex(cond.r.toString())>=0) {
				leftName = cond.r.toString();
				rightName = cond.l.toString();
			} else canMergeJoin = false;
		}
		
		left.eval(env);
		right.eval(env);
		if (!canMergeJoin) fetchNext();
		else {
			LinkedList<Record> lResults = new LinkedList<Record>();
			final int leftIdx = left.getSchema().findIndex(leftName);
			while (left.hasNext())
				lResults.add(left.next());
			Collections.sort(lResults, new Comparator<Record>() {
				public int compare(Record a, Record b) {
					Type l = a.getCol(leftIdx);
					Type r = b.getCol(leftIdx);
					if (l.compWith(BinaryOp.GT, r))	return 1;
					else if (l.compWith(BinaryOp.LT, r))	return -1;
					else return 0;
				}
			});
			
			LinkedList<Record> rResults = new LinkedList<Record>();
			final int rightIdx = right.getSchema().findIndex(rightName);
			while (right.hasNext())
				rResults.add(right.next());
			Collections.sort(rResults, new Comparator<Record>(){
				public int compare(Record a, Record b){
					Type l = a.getCol(rightIdx);
					Type r = b.getCol(rightIdx);
					if (l.compWith(BinaryOp.GT, r)) return 1;
					else if (l.compWith(BinaryOp.LT, r)) return -1;
					else return 0;
				}
			});
			
			if (lResults.isEmpty() || rResults.isEmpty())
				return;
			
			Record curLeft = lResults.pollFirst();
			Record curRight = rResults.pollFirst();
			List<Record> lastRight = new ArrayList<Record>();
			Type lval = curLeft.getCol(leftIdx);
			Type rval = curRight.getCol(rightIdx);
			Type lastrval = null;
			while (true) {
				lval = curLeft.getCol(leftIdx);
				if (lastrval != null && lval.compWith(BinaryOp.EQ, lastrval)){
					for (Record r : lastRight){
						Record tmp = productRecord(curLeft, r);
						if (predTest(tmp)) results.add(tmp);
					}
				} else {
					lastRight = new ArrayList<Record>();
					while (lval.compWith(BinaryOp.GT, rval) && !rResults.isEmpty()) {
						curRight = rResults.pollFirst();
						rval = curRight.getCol(rightIdx);
					}
					while (lval.compWith(BinaryOp.EQ, rval)) {
						Record tmp = productRecord(curLeft, curRight);
						if (predTest(tmp)) results.add(tmp);
						lastRight.add(curRight);
						if (rResults.isEmpty()) break;
						curRight = rResults.pollFirst();
						rval = curRight.getCol(rightIdx);
					}
					lastrval = lval;
				}
				if (lResults.isEmpty()) break;
				curLeft = lResults.pollFirst();
			}
		}
	}

	private BinaryExpr checkMergeJoin() {
		for (Expr e : condList) {
			if (e instanceof BinaryExpr) {
				BinaryExpr now = (BinaryExpr) e;
				if (now.op == BinaryOp.EQ && now.l instanceof ID && now.r instanceof ID)
					return now;
			}
		}
		return null;
	}

	@Override
	public String toString(){
		return "Join (left="+left.toString()+", right="+right.toString()+")";
	}

	private boolean srcHasNext() {
		if (!hasEval) Util.error("ThetaJoin not eval");
		return (curLeft != null || left.hasNext()) && right.hasNext();
	}
	@Override
	public boolean hasNext() {
		return canMergeJoin?ptr < results.size() : current != null;
	}

	@Override
	public Record next() {
		if (!canMergeJoin) {
			Record ret = current;
			fetchNext();
			return ret;
		} else return results.get(ptr++);
	}

	private Record srcNext() {
		if (!hasEval) Util.error("Join not eval");
		Record r = right.next();
		if (curLeft == null)
			curLeft = left.next();
		Record ans = productRecord(curLeft, r);
		if (!right.hasNext() && left.hasNext()) {
			right.reset();
			curLeft = left.next();
		}
		return ans;
	}

	private Record productRecord(Record l, Record r) {
		Record ret = new Record(schema);
		ret.cols.addAll(l.cols);
		ret.cols.addAll(r.cols);
		return ret;
	}
	
	private void fetchNext() {
		Record ans = null;
		while (srcHasNext()) {
			Record r = srcNext();
			if (predTest(r)) {
				ans = r;
				break;
			}
		}
		current = ans;
	}
	private boolean predTest(Record r) {
		Env tmp = env.clone();
		tmp.appendFromRecord(r);
		for (Expr pred : condList)
			if(!pred.evalPred(tmp)) return false;
		
		return true;
	}

	@Override
	public void reset() {
		left.reset();
		right.reset();
		curLeft = null;
		ptr = 0;
		if (!canMergeJoin) {
			current = null;
			fetchNext();
		}
	}

	@Override
	public Schema getSchema() {
		return schema;
	}

	@Override
	public void close() {
		left.close();
		right.close();
	}


	@Override
	public List<String> getColumns() {
		List<String> ans = new LinkedList<String> (left.getColumns());
		ans.addAll(right.getColumns());
		return ans;
	}

	@Override
	public List<String> getRequestedColumns() {
		List<String> ans = new LinkedList<String> (left.getRequestedColumns());
		ans.addAll(right.getRequestedColumns());
		return ans;
	}


	@Override
	public void rename(String oldName, String newName) {
		left.rename(oldName, newName);
		right.rename(oldName, newName);
	}

	public void addCond(Expr pred) {
		condList.add(pred);
	}
}
