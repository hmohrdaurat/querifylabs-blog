package com.querifylabs.blog.optimizer;

import java.util.HashMap;
import java.util.Random;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Util;
import org.apache.calcite.plan.Context;

public class CustomCostPlanner extends VolcanoPlanner {

  final RelOptCost zeroCost;
  final RelOptCost infCost;

  public CustomCostPlanner(RelOptCostFactory costFactory,
      Context externalContext) {
    super(costFactory, externalContext);
    this.zeroCost = this.costFactory.makeZeroCost();
    this.infCost = this.costFactory.makeInfiniteCost();
  }

  @Override
  public RelOptCost getCost(RelNode rel, RelMetadataQuery mq) {
    assert rel != null : "pre-condition: rel != null";
    if (rel instanceof RelSubset) {
      // return BestCost through VolcanoPlanner (only them have access)
      return super.getCost(rel, mq);
    }
    if (/*noneConventionHasInfiniteCost
        &&*/ rel.getTraitSet().getTrait(ConventionTraitDef.INSTANCE) == Convention.NONE) {
      return costFactory.makeInfiniteCost();
    }
    RelOptCost cost = mq.getNonCumulativeCost(rel);
    System.err.println("rel: " + rel.toString() + ", card=" + String.valueOf(cost.getRows())/*+ ", cost=" + String.valueOf(cost.getCpu())*/);
    if (!zeroCost.isLt(cost)) {
      // cost must be positive, so nudge it
      cost = costFactory.makeTinyCost();
    }
    for (RelNode input : rel.getInputs()) {
      cost = cost.plus(getCost(input, mq));
    }
    return cost;
  }

}