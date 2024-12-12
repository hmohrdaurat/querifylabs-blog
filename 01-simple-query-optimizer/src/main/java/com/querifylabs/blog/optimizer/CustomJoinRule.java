package com.querifylabs.blog.optimizer;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Planner rule that converts a
 * {@link org.apache.calcite.rel.logical.LogicalJoin} relational expression
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}.
 *
 * @see EnumerableRules#ENUMERABLE_JOIN_RULE */
class CustomJoinRule extends ConverterRule {
    /** Default configuration. */
    public static final Config DEFAULT_CONFIG = Config.INSTANCE
        .withConversion(LogicalJoin.class, Convention.NONE,
            EnumerableConvention.INSTANCE, "CustomJoinRule")
        .withRuleFactory(CustomJoinRule::new);
  
    /** Called from the Config. */
    protected CustomJoinRule(Config config) {
      super(config);
    }
  
    @Override public RelNode convert(RelNode rel) {
      LogicalJoin join = (LogicalJoin) rel;
      List<RelNode> newInputs = new ArrayList<>();
      for (RelNode input : join.getInputs()) {
        if (!(input.getConvention() instanceof EnumerableConvention)) {
          input =
              convert(
                  input,
                  input.getTraitSet()
                      .replace(EnumerableConvention.INSTANCE));
        }
        newInputs.add(input);
      }
      final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
      final RelNode left = newInputs.get(0);
      final RelNode right = newInputs.get(1);
      final JoinInfo info = join.analyzeCondition();
  
      // If the join has equiKeys (i.e. complete or partial equi-join),
      // create an EnumerableHashJoin, which supports all types of joins,
      // even if the join condition contains partial non-equi sub-conditions;
      // otherwise (complete non-equi-join), create an EnumerableNestedLoopJoin,
      // since a hash join strategy in this case would not be beneficial.
      //final boolean hasEquiKeys = !info.leftKeys.isEmpty()
      //    && !info.rightKeys.isEmpty();
      /*if (hasEquiKeys)*/ {
        // Re-arrange condition: first the equi-join elements, then the non-equi-join ones (if any);
        // this is not strictly necessary but it will be useful to avoid spurious errors in the
        // unit tests when verifying the plan.
        final RexNode equi = info.getEquiCondition(left, right, rexBuilder);
        final RexNode condition;
        if (info.isEqui()) {
          condition = equi;
        } else {
          final RexNode nonEqui = RexUtil.composeConjunction(rexBuilder, info.nonEquiConditions);
          condition = RexUtil.composeConjunction(rexBuilder, Arrays.asList(equi, nonEqui));
        }
        return EnumerableHashJoin.create(
            left,
            right,
            condition,
            join.getVariablesSet(),
            join.getJoinType());
      }
      //return EnumerableNestedLoopJoin.create(
      //    left,
      //    right,
      //    join.getCondition(),
      //    join.getVariablesSet(),
      //    join.getJoinType());
    }
  }
  