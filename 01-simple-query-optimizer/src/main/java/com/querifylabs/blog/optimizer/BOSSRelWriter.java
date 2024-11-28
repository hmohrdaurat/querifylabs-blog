package com.querifylabs.blog.optimizer;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.avatica.util.Spacer;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDigestIncludeType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

public class BOSSRelWriter implements RelWriter {
    protected final PrintWriter pw;
    private final SqlExplainLevel detailLevel;
    private final boolean multiline;
    protected final Spacer spacer = new Spacer();
    private final List<Pair<String, Object>> values = new ArrayList<>();
    private List<String> currentInputAttributes = new ArrayList<>();
    private final List<String> currentSortAttributes = new LinkedList<>();
    private final List<String> currentProjAttributes = new ArrayList<>();

    public BOSSRelWriter(PrintWriter pw) {
        this(pw, SqlExplainLevel.NO_ATTRIBUTES, true);
    }

    public BOSSRelWriter(PrintWriter pw, SqlExplainLevel detailLevel, boolean multiline) {
        this.pw = pw;
        this.detailLevel = detailLevel;
        this.multiline = multiline;
    }
  
    private final String[] opGenericNames = new String[] {"NestedLoopJoin", "HashJoin", "MergeJoin", "Join", "Project", "Sort"};
    private final Map<String, String> strictConversions = new HashMap<String, String>() {{
        put("=", "Equal");
        put("!=", "NotEqual");
        put("<", "Less");
        put("<=", "LessOrEqual");
        put(">", "Greater");
        put(">=", "GreaterOrEqual");
        put("+", "Plus");
        put("-", "Minus");
        put("*", "Multiply");
        put("/", "Divide");
    }};
    private String renameOp(String opName) {
        for(String name : opGenericNames) {
            if(opName.contains(name)) {
                return name;
            }
        }
        // cases where we modify the name
        if(opName.contains("Filter")) {
        return "Select";
        }
        if(opName.contains("Aggregate")) {
            return "Group";
        }
        if(opName.contains("Limit")) {
            return "Top";
        } 
        return strictConversions.getOrDefault(opName, 
            Character.toUpperCase(opName.charAt(0)) + opName.substring(1).toLowerCase());
    }

    @Override
    public void explain(RelNode rel, List<Pair<String, Object>> valueList) {
        List<RelNode> inputs = rel.getInputs();
        final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
        if (!mq.isVisibleInExplain(rel, detailLevel)) {
            // render children in place of this, at same level
            for (RelNode input : inputs) {
                input.explain(this);
            }
            return;
        }

        StringBuilder s = new StringBuilder();
        spacer.spaces(s);
        s.append('(').append(renameOp(rel.getRelTypeName()));  

        if(valueList.isEmpty()) {
            s.append(' ');
        }

        boolean isScanNode = rel.getRelTypeName().contains("TableScan");
        boolean isAProjectionNode = rel.getRelTypeName().contains("Project");
        boolean isAGroupNode = rel.getRelTypeName().contains("Group") || 
                                rel.getRelTypeName().contains("Aggregate");

        AtomicBoolean lastWasNested = new AtomicBoolean(false);
        valueList.forEach(value -> {
            // handle operators and predicates
            if (value.right instanceof RelNode || 
                value.right instanceof RexCall || 
                value.right instanceof AggregateCall) {
                lastWasNested.set(true);
                if(multiline) {
                    s.append(System.lineSeparator());
                } else {
                    s.append(' ');
                }

                // already print what we have so far
                pw.print(s);
                s.setLength(0);

                if(multiline) {
                    spacer.add(4);
                }

                if (value.right instanceof RelNode) {
                    List<String> oldInputAttributes = currentInputAttributes;
                    currentInputAttributes = new ArrayList<>();
                    ((RelNode)value.right).explain(this);
                    oldInputAttributes.addAll(currentInputAttributes);
                    currentInputAttributes = oldInputAttributes;
                } else {
                    spacer.spaces(s);
                    if(isAProjectionNode || isAGroupNode) {
                        s.append("(As \'").append(value.left).append(' ');
                    } else {
                        s.append("(Where ");
                    }                    
                    if(multiline) {
                        s.append(System.lineSeparator());
                        spacer.add(4);
                    }
                    pw.print(s);
                    s.setLength(0);

                    if (value.right instanceof RexCall) {
                        RexCall call = (RexCall)value.right;
                        explain(call.getOperator().getName(), call.getOperands());
                    } else {
                        AggregateCall call = (AggregateCall)value.right;
                        List<RexNode> argAsNodes = new ArrayList<>();
                        RelDataTypeFactory typeFactory =
                            new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
                        call.getArgList().forEach(arg -> argAsNodes.add(
                            new RexInputRef(arg, typeFactory.createSqlType(SqlTypeName.BIGINT))));
                        explain(call.getAggregation().getName(), argAsNodes);
                        currentProjAttributes.add(value.left);
                    }

                    if(multiline) {
                        spacer.subtract(4);
                        s.append(System.lineSeparator());
                        spacer.spaces(s);
                    }
                    s.append(')');
                    pw.print(s);
                    s.setLength(0);

                    currentProjAttributes.add(value.left);
                }

                if(multiline) {
                    spacer.subtract(4);
                }
                return;
            }
            // handle attributes (e.g., for projection)
            if(value.right instanceof RexInputRef) {
                if(isAProjectionNode) {
                    if(multiline) {
                        spacer.add(4);
                        s.append(System.lineSeparator());
                        spacer.spaces(s);
                    } else {
                        s.append(' ');
                    }
                    s.append("(As \'").append(value.left);
                }
                // retrieve actual attribute name
                int attrIndex = ((RexInputRef) value.right).getIndex();
                String attr = currentInputAttributes.get(attrIndex);                
                if(!isAProjectionNode && value.left.startsWith("sort")) {
                    // special handling for sort attributes:
                    // keep it for later (to re-match with Asc/Desc)
                    currentSortAttributes.add(attr);
                } else {
                    s.append(" \'").append(attr);
                }
                if(isAProjectionNode) {
                    s.append(')');
                    if(multiline) {
                        spacer.subtract(4);
                    }
                }
                currentProjAttributes.add(value.left);
                return;
            }
            if(value.left.startsWith("dir")) {
                String sortAttr = currentSortAttributes.get(0);
                currentSortAttributes.remove(0);
                s.append(" \'").append(sortAttr).append(" \'").append(value.right);
                return;
            }
            // handling group attributes
            if(value.left.equals("group")) {
                ImmutableBitSet group = (ImmutableBitSet)value.right;
                if(group.isEmpty()) {
                    return;
                }
                if(multiline) {
                    spacer.add(4);
                    s.append(System.lineSeparator());
                    spacer.spaces(s);
                } else {
                    s.append(' ');
                }
                s.append("(By");
                for(int i = 0; i < group.length(); i++) {
                    if(group.get(i)) {
                        s.append(" \'").append(currentInputAttributes.get(i));
                        currentProjAttributes.add(currentInputAttributes.get(i));
                    }
                }
                s.append(')');
                if(multiline) {
                    spacer.subtract(4);
                }
                return;
            }
            // handle table name
            if(value.left.equals("table")) {
                currentInputAttributes.addAll(rel.getRowType().getFieldNames());
                ImmutableList<String> tableInfo = ((ImmutableList<String>)value.right);
                // replace the whole node: we don't want to keep a "TableScan" wrapper node
                // (but do that only once: for the first argument...)
                if(isScanNode && s.toString().contains("tablescan")) {
                    s.setLength(0);
                    spacer.spaces(s);
                } else {
                    s.append(' ');
                }
                s.append('\'').append(tableInfo.get(1)); // 0: db, 1: table
                return;
            }
            if(detailLevel == SqlExplainLevel.ALL_ATTRIBUTES) {
                // handle other attributes
                lastWasNested.set(false);
                s.append(' ').append(value.left).append(':').append(value.right);
            }
        });

        if(isAProjectionNode || isAGroupNode) {
            currentInputAttributes.clear();
            currentInputAttributes.addAll(currentProjAttributes);
        }
        currentProjAttributes.clear();
        
        if(multiline  && lastWasNested.get()) {
            s.append(System.lineSeparator());
            spacer.spaces(s);
        }
        if(!isScanNode) {
            // no need, we transformed the node into a single argument already
            s.append(')');
        }
        pw.print(s);
    }

    private void explain(String operatorName, List<RexNode> operands) {        
        StringBuilder s = new StringBuilder();
        spacer.spaces(s);
        s.append('(').append(renameOp(operatorName)); 

        if(operands.isEmpty()) {
            s.append(' ');
        }

        // special case for "(EXTRACT (FLAG YEAR) xx)"
        // transform into (YEAR XX)
        if(operatorName.equals("EXTRACT")) {
            String subOp = ((RexLiteral)operands.get(0)).getValue().toString();
            List<RexNode> subArgs = operands.subList(1, operands.size());
            explain(subOp, subArgs);
            return;
        }

        AtomicBoolean lastWasNested = new AtomicBoolean(false);
        operands.forEach(operand -> {
            if (operand instanceof RexCall) { 
                lastWasNested.set(true);
                if(multiline) {
                    s.append(System.lineSeparator());
                } else {
                    s.append(' ');
                }

                // already print what we have so far
                pw.print(s);
                s.setLength(0);

                if(multiline) {
                    spacer.add(4);
                }
                RexCall call = (RexCall)operand;
                explain(call.getOperator().getName(), call.getOperands());
                if(multiline) {
                    spacer.subtract(4);
                }                
                return;
            }
            lastWasNested.set(false);
            // handle literals
            if(operand instanceof RexLiteral) {
                String value = ((RexLiteral) operand).computeDigest(RexDigestIncludeType.NO_TYPE);
                s.append(' ').append(value.replaceAll("\'", "\""));             
                return;
            }
            // handle attributes
            if(operand instanceof RexInputRef) {
                // retrieve actual attribute name
                int attrIndex = ((RexInputRef) operand).getIndex();
                String attr = currentInputAttributes.get(attrIndex);
                s.append(" \'").append(attr);
                return;
            }
            // handle any other type of attribute
            s.append(' ').append(operand.toString());
        });
        
        if(multiline  && lastWasNested.get()) {
            s.append(System.lineSeparator());
            spacer.spaces(s);
        }
        s.append(')');
        pw.print(s);
    }
    
    @Override
    public SqlExplainLevel getDetailLevel() {
        return this.detailLevel;
    }

    @Override
    public RelWriter item(String term, Object value) {
        values.add(Pair.of(term, value));
        return this;
    }

    @Override
    public RelWriter done(RelNode node) {        
        assert checkInputsPresentInExplain(node);
        final List<Pair<String, Object>> valuesCopy =
            ImmutableList.copyOf(values);
        values.clear();
        explain(node, valuesCopy);
        pw.flush();
        return this;
    }
    
    private boolean checkInputsPresentInExplain(RelNode node) {
        int i = 0;
        if (values.size() > 0 && values.get(0).left.equals("subset")) {
            ++i;
        }
        for (RelNode input : node.getInputs()) {
            assert values.get(i).right == input;
            ++i;
        }
        return true;
    }
}
