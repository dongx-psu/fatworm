package fatworm.optimize;

import fatworm.logicplan.Plan;

public class Optimize {
	public static Plan optimize(Plan plan){
		SelectCleanup.process(plan);
		plan = SelectPusher.process(plan);
		JoinFormer.process(plan);
		InnerOrderCleanup.init();
		InnerOrderCleanup.process(plan);
		return plan;
	}
}