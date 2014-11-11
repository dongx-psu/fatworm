package fatworm.logicplan;

import java.util.LinkedList;
import java.util.List;

import fatworm.expr.BinaryExpr;
import fatworm.expr.Expr;
import fatworm.table.Record;
import fatworm.table.Schema;
import fatworm.util.Env;
import fatworm.util.Util;

public class Select extends Plan {
	public Plan src;

	public Expr pred;
	public Env env;
	public Record current;
	public boolean hasPushed;
	public boolean flag = false;

	public Select(Plan src, Expr pred) {
		super();
		this.src = src;
		if(pred instanceof BinaryExpr){
			this.pred = ((BinaryExpr)pred).toCNF();
		} else {
			this.pred = pred;
		}
		src.parent = this;
		myAggr.addAll(this.src.getAggr());
		hasPushed = false;
	}

	@Override
	public String toString(){
		return "select (from="+src.toString()+", where="+pred.toString()+")";
	}

	@Override
	public void eval(Env env) {
		hasEval = true;
		current = null;
		src.eval(env);
		this.env = env;
		fetchNext();
	}

	private void fetchNext() {
		if (flag) {
			current = src.hasNext() ? src.next() : null;
			return;
		}
		Record ans = null;
		while (src.hasNext()) {
			Record r = src.next();
			Env tmp = env.clone();
			tmp.appendFromRecord(r);

			if (pred.evalPred(tmp)) {
				ans = r;
				break;
			}
		}
		current = ans;
	}

	@Override
	public boolean hasNext() {
		return current != null;
	}

	@Override
	public Record next() {
		Record ret = current;
		fetchNext();
		return ret;
	}

	@Override
	public void reset() {
		src.reset();
		current = null;
		fetchNext();
	}

	@Override
	public Schema getSchema() {
		return src.getSchema();
	}

	@Override
	public void close() {
		src.close();
	}

	@Override
	public List<String> getColumns() {
		return new LinkedList<String> (src.getColumns());
	}

	@Override
	public List<String> getRequestedColumns() {
		List<String> ans = src.getRequestedColumns();
		ans.removeAll(src.getColumns());
		Util.addAllCol(ans, pred.getRequestedColumns());
		return ans;
	}

	@Override
	public void rename(String oldName, String newName) {
		src.rename(oldName, newName);
		if (pred.getType(src.getSchema()) == java.sql.Types.NULL)
			pred.rename(oldName, newName);
	}

	public boolean canPush() {
		if (pred.hasSubquery())
			return false;
		if (src instanceof Group || src instanceof FetchTable || src instanceof ConstPlan || src instanceof None)
			return false;
		if (src instanceof Join || src instanceof Rename )
			return false;
		if (src instanceof Project)
			return ((Project)src).isConst();
		if (src instanceof Select || src instanceof TableRename)
			return true;
		if (src instanceof Product)
			return Util.subsetof(pred.getRequestedColumns(), ((Product)src).left.getColumns()) || Util.subsetof(pred.getRequestedColumns(), ((Product)src).right.getColumns());
		
		return false;
	}
}
