package com.querifylabs.blog.optimizer;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.enumerable.EnumerableHashJoin;
import org.apache.calcite.adapter.tpch.TpchSchema;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptRules;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateUnionTransposeRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.util.TestUtil;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

public class OptimizerTest {
    @Test
    public void test_tpch_q6() throws Exception {
        String sql =
            "select\n" +
            "    sum(l.L_EXTENDEDPRICE * l.L_DISCOUNT) as revenue\n" +
            "from\n" +
            "    LINEITEM l\n" +
            "where\n" +
            "    L_SHIPDATE >= ?\n" +
            "    and L_SHIPDATE < ?\n" +
            "    and L_DISCOUNT between (? - 0.01) AND (? + 0.01)\n" +
            "    and L_QUANTITY < ?";
        test_query(sql);
    }

    @Test
    public void test_tpch_q9() throws Exception {
        String sql =
        "select                                                                                            \n" +
        "        nation,                                                                                   \n" +
        "        o_year,                                                                                   \n" +
        "        sum(amount) as sum_profit                                                                 \n" +
        "	from                                                                                           \n" +
        "        (                                                                                         \n" +
        "                select                                                                            \n" +
        "                        N_NAME as nation,                                                         \n" +
        "                        year(O_ORDERDATE) as o_year,                                              \n" +
        "                        L_EXTENDEDPRICE * (1 - L_DISCOUNT) - PS_SUPPLYCOST * L_QUANTITY as amount \n" +
        "                from                                                                              \n" +
        "                        PART,                                                                     \n" +
        "                        SUPPLIER,                                                                 \n" +
        "                        LINEITEM,                                                                 \n" +
        "                        PARTSUPP,                                                                 \n" +
        "                        ORDERS,                                                                   \n" +
        "                        NATION                                                                    \n" +
        "                where                                                                             \n" +
        "				L_SUPPKEY = S_SUPPKEY                                                              \n" +
        "				and L_PARTKEY = P_PARTKEY                                                          \n" +
        "				and PS_PARTKEY = P_PARTKEY                                                         \n" +
        "				and PS_SUPPKEY = S_SUPPKEY                                                         \n" +
        "				and P_NAME like '%green%'                                                          \n" +
        "        ) as profit                                                                               \n" +
        "	group by                                                                                       \n" +
        "			nation,                                                                                \n" +
        "			o_year                                                                                 \n" +
        "	order by                                                                                       \n" +
        "			nation,                                                                                \n" +
        "			o_year desc";
        test_query(sql);
    }

    private void test_query(String sql) throws Exception {
        TpchSchema tpchSchema = new TpchSchema(1.0, 1, 1, true);
        Optimizer optimizer = Optimizer.create("tpch", tpchSchema);

        SqlNode sqlTree = optimizer.parse(sql);
        SqlNode validatedSqlTree = optimizer.validate(sqlTree);
        RelNode relTree = optimizer.convert(validatedSqlTree);

        print("AFTER CONVERSION", relTree);

        
        /*if(true) {
            SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(config.lenientOperatorLookup())
                .withSqlConformance(config.conformance())
                .withDefaultNullCollation(config.defaultNullCollation())
                .withIdentifierExpansion(true);
            VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(config));                
            planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
            RelOptCluster cluster = relTree.getCluster();
            VolcanoPlanner planner = (VolcanoPlanner) cluster.getPlanner();
            RelTraitSet desiredTraits = cluster.traitSet().replace(EnumerableConvention.INSTANCE);
            RelNode newRoot = planner.changeTraits(relTree, desiredTraits);
            planner.setRoot(newRoot);
            RelOptUtil.registerDefaultRules(planner, false, false);
            RelNode optimizerRelTree = planner.findBestExp();            
            print("AFTER OPTIMIZATION", optimizerRelTree);
            return;
        }*/

        RuleSet rules = RuleSets.ofList(
            CoreRules.FILTER_TO_CALC,
            CoreRules.PROJECT_TO_CALC,
            CoreRules.FILTER_CALC_MERGE,
            CoreRules.PROJECT_CALC_MERGE,
            //CoreRules.JOIN_TO_MULTI_JOIN,
            //CoreRules.MULTI_JOIN_OPTIMIZE,
            // enumerable rules
            EnumerableRules.ENUMERABLE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
            EnumerableRules.ENUMERABLE_CORRELATE_RULE,
            EnumerableRules.ENUMERABLE_PROJECT_RULE,
            EnumerableRules.ENUMERABLE_FILTER_RULE,
            EnumerableRules.ENUMERABLE_CALC_RULE,
            EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
            EnumerableRules.ENUMERABLE_SORT_RULE,
            EnumerableRules.ENUMERABLE_LIMIT_RULE,
            EnumerableRules.ENUMERABLE_COLLECT_RULE,
            EnumerableRules.ENUMERABLE_UNCOLLECT_RULE,
            EnumerableRules.ENUMERABLE_UNION_RULE,
            EnumerableRules.ENUMERABLE_REPEAT_UNION_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SPOOL_RULE,
            EnumerableRules.ENUMERABLE_INTERSECT_RULE,
            EnumerableRules.ENUMERABLE_MINUS_RULE,
            EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
            EnumerableRules.ENUMERABLE_VALUES_RULE,
            EnumerableRules.ENUMERABLE_WINDOW_RULE,
            EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
            EnumerableRules.ENUMERABLE_TABLE_FUNCTION_SCAN_RULE,
            EnumerableRules.ENUMERABLE_MATCH_RULE
        );

        RelNode optimizerRelTree = optimizer.optimize(
            relTree,
            relTree.getTraitSet().plus(EnumerableConvention.INSTANCE),
            rules
        );

        print("AFTER OPTIMIZATION", optimizerRelTree);
    }

    private void print(String header, RelNode relTree) {
        StringWriter sw = new StringWriter();

        sw.append(header).append(":").append("\n");

        RelWriterImpl relWriter = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.ALL_ATTRIBUTES, true);

        relTree.explain(relWriter);

        System.out.println(sw.toString());
    }
}
