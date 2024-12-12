package com.querifylabs.blog.optimizer;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RuleSet;

import com.google.common.collect.ImmutableList;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Optimizer {

    private final CalciteConnectionConfig config;
    private final SqlValidator validator;
    private final SqlToRelConverter converter;
    private final RelOptPlanner planner;
    private final Prepare.CatalogReader catalogReader;

    public Optimizer(
        CalciteConnectionConfig config,
        SqlValidator validator,
        SqlToRelConverter converter,
        RelOptPlanner planner,
        Prepare.CatalogReader catalogReader
    ) {
        this.config = config;
        this.validator = validator;
        this.converter = converter;
        this.planner = planner;
        this.catalogReader = catalogReader;
    }
    
  static final List<RelOptRule> NEEDED_BASE_RULES = ImmutableList.of(
      //CoreRules.PROJECT_MERGE,
      CoreRules.FILTER_INTO_JOIN,
      CoreRules.JOIN_COMMUTE,
      //CoreRules.JOIN_ASSOCIATE,
      JoinPushThroughJoinRule.RIGHT,
      JoinPushThroughJoinRule.LEFT
    );

    public static Optimizer create(String schemaName, Schema schema) {
        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

        Properties configProperties = new Properties();
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);

        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false, schemaName, schema);
        Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
            rootSchema,
            Collections.singletonList(schemaName),
            typeFactory,
            config
        );

        SqlOperatorTable operatorTable = /*ChainedSqlOperatorTable.of*/(SqlStdOperatorTable.instance());
        //ListSqlOperatorTable opList = new ListSqlOperatorTable();
        //opList.add(new SqlFilterOperator());
        //SqlOperatorTable operatorTable = opList;

        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
            .withLenientOperatorLookup(config.lenientOperatorLookup())
            .withSqlConformance(config.conformance())
            .withDefaultNullCollation(config.defaultNullCollation())
            .withIdentifierExpansion(true);

        SqlValidator validator = SqlValidatorUtil.newValidator(operatorTable, catalogReader, typeFactory, validatorConfig);

        CustomCostPlanner planner = new CustomCostPlanner(CustomCost.FACTORY, Contexts.of(config));            
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        RelOptUtil.registerDefaultRules(planner, false, false);
        //NEEDED_BASE_RULES.forEach(planner::addRule);
        planner.removeRule(CoreRules.PROJECT_MERGE);
        planner.removeRule(CoreRules.JOIN_ASSOCIATE);
        EnumerableRules.ENUMERABLE_RULES.forEach(planner::addRule);
        planner.addRule(EnumerableRules.TO_INTERPRETER);
        //planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
        //planner.addRule(CustomJoinRule.DEFAULT_CONFIG.toRule(CustomJoinRule.class));

        RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
            .withTrimUnusedFields(true)
            .withExpand(false) // https://issues.apache.org/jira/browse/CALCITE-1045
            ;//.build();

        SqlToRelConverter converter = new SqlToRelConverter(
            null,
            validator,
            catalogReader,
            cluster,
            StandardConvertletTable.INSTANCE,
            converterConfig
        );

        return new Optimizer(config, validator, converter, planner, catalogReader);
    }

    public SqlNode parse(String sql) throws Exception {
        SqlParser.ConfigBuilder parserConfig = SqlParser.configBuilder();
        parserConfig.setCaseSensitive(config.caseSensitive());
        parserConfig.setUnquotedCasing(config.unquotedCasing());
        parserConfig.setQuotedCasing(config.quotedCasing());
        parserConfig.setConformance(config.conformance());

        SqlParser parser = SqlParser.create(sql, parserConfig.build());

        return parser.parseStmt();
    }

    public SqlNode validate(SqlNode node) {
        return validator.validate(node);
    }

    public RelNode convert(SqlNode node) {
        RelRoot root = converter.convertQuery(node, false, true);

        return root.rel;
    }

    private class CustomBuilder extends RelBuilder {
        public CustomBuilder() {
            super(planner.getContext(), converter.getCluster(), catalogReader);
        }
    }

    public RelBuilder builder() {
        return new CustomBuilder();
    }

    public RelNode optimize(RelNode node, RelTraitSet requiredTraitSet, RuleSet rules) {
        /*Program program = Programs.of(RuleSets.ofList(rules));

        return program.run(
            planner,
            node,
            requiredTraitSet,
            Collections.emptyList(),
            Collections.emptyList()
        );*/

        for (RelOptRule rule : rules) {
            planner.addRule(rule);
        }

        planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
        //planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);

        // for tests
        //planner.removeRule(CoreRules.SORT_JOIN_TRANSPOSE);
        //planner.removeRule(CoreRules.SORT_PROJECT_TRANSPOSE);
        //planner.removeRule(CoreRules.SORT_REMOVE);
        //planner.removeRule(CoreRules.SORT_JOIN_COPY);
        
        print("LOGICAL PLAN", node);

        RelNode newRoot = planner.changeTraits(node, requiredTraitSet);
        planner.setRoot(newRoot);
        RelNode optimized = planner.findBestExp();

        print("AFTER OPTIMIZATION", optimized);

        //System.out.println("optimized cost: " + optimized.getCluster().getMetadataQuery().getCumulativeCost(optimized));
        
        return optimized;
    }

    public RelNode optimizeNoDebugOutput(RelNode node, RelTraitSet requiredTraitSet) {
        /*Program program = Programs.of(RuleSets.ofList(rules));

        return program.run(
            planner,
            node,
            requiredTraitSet,
            Collections.emptyList(),
            Collections.emptyList()
        );*/

        planner.removeRule(EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE);
        //planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);

        // for tests
        //planner.removeRule(CoreRules.SORT_JOIN_TRANSPOSE);
        //planner.removeRule(CoreRules.SORT_PROJECT_TRANSPOSE);
        //planner.removeRule(CoreRules.SORT_REMOVE);
        //planner.removeRule(CoreRules.SORT_JOIN_COPY);
        
        //print("LOGICAL PLAN", node);

        RelNode newRoot = planner.changeTraits(node, requiredTraitSet);
        planner.setRoot(newRoot);
        RelNode optimized = planner.findBestExp();

        //print("AFTER OPTIMIZATION", optimized);

        //System.out.println("optimized cost: " + optimized.getCluster().getMetadataQuery().getCumulativeCost(optimized));
        
        return optimized;
    }

    private void print(String header, RelNode relTree) {
        StringWriter sw = new StringWriter();

        sw.append(header).append(":").append("\n");

        //RelWriterImpl relWriter = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.ALL_ATTRIBUTES, true);
        BOSSRelWriter relWriter = new BOSSRelWriter(new PrintWriter(sw));

        relTree.explain(relWriter);
        System.out.println(sw.toString());
    }
}
