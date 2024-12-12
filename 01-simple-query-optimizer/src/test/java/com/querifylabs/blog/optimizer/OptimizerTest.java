package com.querifylabs.blog.optimizer;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.tpch.TpchSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class OptimizerTest {

    private Optimizer createOptimizer() {
        TpchSchema tpchSchema = new TpchSchema(10.0, 1, 1, true);
        return Optimizer.create("tpch", tpchSchema);
    }

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
        "				and O_ORDERKEY = L_ORDERKEY                                                        \n" +
        "				and S_NATIONKEY = N_NATIONKEY                                                      \n" +
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

    // QO able to use a Sort-Merge Join for this query
    @Test
    public void test_sort_example_A() throws Exception {
        String sql =
        "   select                                                                            \n" +
        "           C_NAME as name,                                                           \n" +
        "           O_ORDERDATE                                                               \n" +
        "   from                                                                              \n" +
        "           ORDERS,                                                                   \n" +
        "           CUSTOMER                                                                  \n" +
        "   where                                                                             \n" +
        "	        O_CUSTKEY = C_CUSTKEY                                                     \n" +
        "	order by                                                                          \n" +
        "			O_CUSTKEY asc";
        test_query(sql);
    }

    // QO unable to use a Sort-Merge Join (because asc/desc mismatch)
    @Test
    public void test_sort_example_B() throws Exception {
        String sql =
        "   select                                                                            \n" +
        "           C_NAME as name,                                                           \n" +
        "           O_ORDERDATE                                                               \n" +
        "   from                                                                              \n" +
        "           ORDERS,                                                                   \n" +
        "           CUSTOMER                                                                  \n" +
        "   where                                                                             \n" +
        "	        O_CUSTKEY = C_CUSTKEY                                                     \n" +
        "	order by                                                                          \n" +
        "			O_CUSTKEY desc";
        test_query(sql);
    }

    // QO rightfully not using Sort-Merge Join (because key is different from the order attribute)
    @Test
    public void test_sort_example_C() throws Exception {
        String sql =
        "   select                                                                            \n" +
        "           C_NAME as name,                                                           \n" +
        "           O_ORDERDATE                                                               \n" +
        "   from                                                                              \n" +
        "           ORDERS,                                                                   \n" +
        "           CUSTOMER                                                                  \n" +
        "   where                                                                             \n" +
        "	        O_CUSTKEY = C_CUSTKEY                                                     \n" +
        "	order by                                                                          \n" +
        "			O_ORDERKEY";
        test_query(sql);
    }

    // QO unable to use Sort-Merge Join (because key is a subset of the order attributes)
    @Test
    public void test_sort_example_D() throws Exception {
        String sql =
        "   select                                                                            \n" +
        "           C_NAME as name,                                                           \n" +
        "           O_ORDERDATE                                                               \n" +
        "   from                                                                              \n" +
        "           ORDERS,                                                                   \n" +
        "           CUSTOMER                                                                  \n" +
        "   where                                                                             \n" +
        "	        O_CUSTKEY = C_CUSTKEY                                                     \n" +
        "	order by                                                                          \n" +
        "			O_CUSTKEY, O_ORDERKEY";
        test_query(sql);
    }
    
    // QO able to use a Sort-Merge Join for this query
    @Test
    public void test_sort_larger_A() throws Exception {
        String sql =
        "   select                                                                            \n" +
        "           O_ORDERKEY,                                                               \n" +
        "           O_ORDERDATE                                                               \n" +
        "   from                                                                              \n" +
        "           ORDERS,                                                                   \n" +
        "           LINEITEM                                                                  \n" +
        "   where                                                                             \n" +
        "	        O_ORDERKEY = L_ORDERKEY                                                   \n" +
        "	order by                                                                          \n" +
        "			L_ORDERKEY asc";
        test_query(sql);
    }
    
    // QO rightfully not using Sort-Merge Join (because key is different from the order attribute)
    @Test
    public void test_sort_larger_C() throws Exception {
        String sql =
        "   select                                                                            \n" +
        "           O_ORDERKEY,                                                               \n" +
        "           O_ORDERDATE                                                               \n" +
        "   from                                                                              \n" +
        "           ORDERS,                                                                   \n" +
        "           LINEITEM                                                                  \n" +
        "   where                                                                             \n" +
        "	        O_ORDERKEY = L_ORDERKEY                                                   \n" +
        "	order by                                                                          \n" +
        "			O_CUSTKEY asc";
        test_query(sql);
    }

    @Test
    public void test_manual_query() throws Exception {
        Optimizer optimizer = createOptimizer();
        RelBuilder builder = optimizer.builder();
        RelNode opTree = builder.scan("ORDERS")
        .scan("CUSTOMER")
        .join(JoinRelType.INNER, builder.equals(
            builder.field(2, 0, "O_CUSTKEY"), 
            builder.field(2, 1, "C_CUSTKEY")))
        .sort(builder.field("O_CUSTKEY"))
        .project(builder.field("C_NAME"), 
                 builder.field("O_ORDERDATE"))
        .build();
        test_query(opTree, optimizer);
    }

    private void test_query(String sql) throws Exception {
        Optimizer optimizer = createOptimizer();
        SqlNode sqlTree = optimizer.parse(sql);
        SqlNode validatedSqlTree = optimizer.validate(sqlTree);
        RelNode relTree = optimizer.convert(validatedSqlTree);
        test_query(relTree, optimizer);
    }
        
    private void test_query(RelNode relTree, Optimizer optimizer) throws Exception {
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
    }

    // https://stackoverflow.com/a/10305419
    public static <E> List<List<E>> possiblePermutations(List<E> original) {
        if (original.isEmpty()) {
            List<List<E>> result = new ArrayList<>();
            result.add(new ArrayList<>());
            return result;
        }
        E firstElement = original.remove(0);
        List<List<E>> returnValue = new ArrayList<>();
        List<List<E>> permutations = possiblePermutations(original);
        for (List<E> smallerPermutated : permutations) {
            for (int index = 0; index <= smallerPermutated.size(); index++) {
                List<E> temp = new ArrayList<>(smallerPermutated);
                temp.add(index, firstElement);
                returnValue.add(temp);
            }
        }
        return returnValue;
    }

    private List<String> createFromClausePermutations(String fromClause) {
        List<String> tables = new ArrayList<String>(Arrays.asList(fromClause.split(",")));
        List<List<String>> tablePermutations = possiblePermutations(tables);
        List<String> permutations = new ArrayList<>();
        for(List<String> permutation : tablePermutations) {
            permutations.add(String.join(", ", permutation));
        }
        return permutations;
    }

    private List<String> generatePermutations(String sql) throws Exception {
        List<String> permutations = new ArrayList<>();

        // get permutations for the first sub-query
        int startIndexOfSubQuery = sql.indexOf("(");
        int endIndexOfSubQuery = -1;
        if(startIndexOfSubQuery >= 0) {
            // find matching closing bracket
            int bracketDepth = 0;
            for(int index = startIndexOfSubQuery + 1; index < sql.length(); index++) {
                char c = sql.charAt(index);
                if(c == '(') {
                    bracketDepth++;
                } else if(c == ')') {
                    if(bracketDepth == 0) {
                        endIndexOfSubQuery = index;
                        break;
                    }
                    bracketDepth--;
                }
            }

            if(endIndexOfSubQuery >= 0) {
                String subQuery = sql.substring(startIndexOfSubQuery + 1, endIndexOfSubQuery);
                List<String> subPermutations = generatePermutations(subQuery);
                String prefix = sql.substring(0, startIndexOfSubQuery + 1);
                for (int i = 0; i < subPermutations.size(); i++) {
                    subPermutations.set(i, prefix + subPermutations.get(i));
                }

                // get permutations for the rest of the query
                // + create combinations from the first sub-query's permutations
                if(endIndexOfSubQuery < sql.length()) {
                    String restQuery = sql.substring(endIndexOfSubQuery);
                    List<String> restPermutations = generatePermutations(restQuery);
                    for (int i = 0; i < subPermutations.size(); i++) {
                        for (int j = 0; j < restPermutations.size(); j++) {
                            permutations.add(i, subPermutations.get(i) + restPermutations.get(j));
                        }
                    }
                    return permutations;
                } else {
                    return subPermutations;
                }
            }
        }

        // check from clause
        int startIndexOfFromClause = sql.toLowerCase().indexOf("from");
        if(startIndexOfFromClause < 0) {
            permutations.add(sql);
            return permutations;
        }
        int endIndexOfFromClause = sql.toLowerCase().indexOf(" where");
        if(endIndexOfFromClause < 0) {
            endIndexOfFromClause = sql.length();
        }
        String fromClause = sql.substring(startIndexOfFromClause + 5, endIndexOfFromClause);
        List<String> fromClausePermutations = createFromClausePermutations(fromClause);
        for(String fromClausePermutation : fromClausePermutations) {
            String prefix = sql.substring(0, startIndexOfFromClause + 5);
            String suffix = sql.substring(endIndexOfFromClause);
            permutations.add(prefix + fromClausePermutation + suffix);
        }
        return permutations;
    }

    List<String> generatePhysicalPlans(String BOSSPlan) {
        List<String> plans = new ArrayList<>();
        int startIndexOfSubQuery = BOSSPlan.indexOf("(HashJoin");
        if(startIndexOfSubQuery < 0) {
            plans.add(BOSSPlan);
            return plans;
        }
        String before = BOSSPlan.substring(0, startIndexOfSubQuery);
        String after = BOSSPlan.substring(startIndexOfSubQuery + 9);
        List<String> afterCandidates = generatePhysicalPlans(after);
        for(String afterCandidate : afterCandidates) {
            plans.add(before + "(HashJoin" + afterCandidate);
            plans.add(before + "(SortMergeJoin" + afterCandidate);
            plans.add(before + "(NestedLoopJoin" + afterCandidate);
        }
        return plans;
    }

    private List<String> generateBOSSQueryPlans(String sql) throws Exception {
        Set<String> plans = new HashSet<String>(); // make sure there is no duplicate plan

        // TODO: parse "From" tables and create all permutations
        sql = sql.replaceAll("\\s+", " ").replaceAll("(\\r|\\n)", "");
        List<String> sqlPermutations = /*new ArrayList<String>(); sqlPermutations.add(sql); //*/generatePermutations(sql);
        System.err.println("number of permutations: " + sqlPermutations.size());

        //String last = sqlPermutations.get(sqlPermutations.size() - 1);
        //sqlPermutations.clear();
        //sqlPermutations.add(last);

        //String first = sqlPermutations.get(0);
        //sqlPermutations.clear();
        //sqlPermutations.add(first);

        Optimizer optimizer = createOptimizer();        
        for(String sqlPermutation : sqlPermutations) {
            // logical plan
            SqlNode sqlTree = optimizer.parse(sqlPermutation);
            SqlNode validatedSqlTree = optimizer.validate(sqlTree);
            RelNode relTree = optimizer.convert(validatedSqlTree);
    
            // physical plan (but allow only one sort of join)
            RelNode optimizerRelTree = optimizer.optimizeNoDebugOutput(
                relTree,
                relTree.getTraitSet().plus(EnumerableConvention.INSTANCE)
            );

            StringWriter sw = new StringWriter();
            BOSSRelWriter relWriter = new BOSSRelWriter(new PrintWriter(sw), SqlExplainLevel.NO_ATTRIBUTES, false);
            //RelWriterImpl relWriter = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.ALL_ATTRIBUTES, true);
            optimizerRelTree.explain(relWriter);

            // drop any NestedLoopJoin, since Calcite use them for cross joins (which are definitely plans we want to prune)
            if(sw.toString().contains("NestedLoopJoin")) {
                continue;
            }

            // generate all plan candidate with different physical ops for each op (only joins for now)
            List<String> candidates = generatePhysicalPlans(sw.toString());

            for(String candidate : candidates) {
                plans.add(candidate);
            }
        }

        System.err.println("number of unique plans: " + plans.size());

        return new ArrayList<>(plans);
    }

    @Test
    public void testGeneratePlans() throws Exception {
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
        "				and O_ORDERKEY = L_ORDERKEY                                                        \n" +
        "				and S_NATIONKEY = N_NATIONKEY                                                      \n" +
        //"				and P_NAME like '%green%'                                                          \n" +
        "        ) as profit                                                                               \n" +
        "	group by                                                                                       \n" +
        "			nation,                                                                                \n" +
        "			o_year                                                                                 \n" +
        "	order by                                                                                       \n" +
        "			nation,                                                                                \n" +
        "			o_year desc";

        List<String> plans = generateBOSSQueryPlans(sql);
        for (String plan : plans) {
            System.out.println(plan);
        }
    }
    
    @Test
    public void testGeneratePlansFromAndToCSV() throws Exception {
        String filedir = "C:\\Users\\ham219\\Downloads\\";
        List<String> filenames = Arrays.asList(
            "tpch_sql_queries.csv"
        );
        
        String outfilename = "tpch_boss_query_plans.csv";

        String line = "";  
        String separator = ",";

        try   
        {
            PrintWriter pw = new PrintWriter(filedir + outfilename);

            int queryIndex = 1;
            for(String filename : filenames) {
                BufferedReader br = new BufferedReader(new FileReader(filedir + filename));  
                while ((line = br.readLine()) != null)
                {  
                    if(queryIndex == 8 || queryIndex == 11 || queryIndex == 15) {
                        queryIndex++;
                        continue;   // not working. TODO: investigate
                                    // Q8: crash (join many tables)
                                    // Q11: error while parsing 'VALUE'
                                    // Q15: error parsing 'CREATE' (calcite not handling views?)
                    }
                    List<String> plans = generateBOSSQueryPlans(line.substring(0, line.length() - 1));
                    for (String plan : plans) {
                        pw.println(queryIndex + separator + plan);
                    }
                    pw.flush();

                    queryIndex++;
                }
                br.close();
            }
            pw.close();
        }   
        catch (IOException e)   
        {  
            e.printStackTrace();  
        }  
    }
}
