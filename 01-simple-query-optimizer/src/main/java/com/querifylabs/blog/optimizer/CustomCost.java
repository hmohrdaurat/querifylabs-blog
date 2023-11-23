package com.querifylabs.blog.optimizer;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptUtil;

import java.util.Objects;

public class CustomCost implements RelOptCost {
    //~ Static fields/initializers ---------------------------------------------

  static final CustomCost INFINITY =
      new CustomCost(
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY,
          Double.POSITIVE_INFINITY) {
        public String toString() {
          return "{inf}";
        }
      };

  static final CustomCost HUGE =
      new CustomCost(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE) {
        public String toString() {
          return "{huge}";
        }
      };

  static final CustomCost ZERO =
      new CustomCost(0.0, 0.0, 0.0) {
        public String toString() {
          return "{0}";
        }
      };

  static final CustomCost TINY =
      new CustomCost(1.0, 1.0, 0.0) {
        public String toString() {
          return "{tiny}";
        }
      };

  public static final RelOptCostFactory FACTORY = new Factory();

  final boolean UseCPUCostOnly = false;//true;

  //~ Instance fields --------------------------------------------------------

  final double cpu;
  final double io;
  final double rowCount;

  //~ Constructors -----------------------------------------------------------

  CustomCost(double rowCount, double cpu, double io) {
    this.rowCount = rowCount;
    this.cpu = cpu;
    this.io = io;
  }

  //~ Methods ----------------------------------------------------------------

  public double getCpu() {
    return cpu;
  }

  public boolean isInfinite() {
    return (this == INFINITY)
        || (this.rowCount == Double.POSITIVE_INFINITY)
        || (this.cpu == Double.POSITIVE_INFINITY)
        || (this.io == Double.POSITIVE_INFINITY);
  }

  public double getIo() {
    return io;
  }

  public boolean isLe(RelOptCost other) {
    CustomCost that = (CustomCost) other;
    if (UseCPUCostOnly) {
      return this == that
          || this.cpu <= that.cpu;
    }
    return (this == that)
        || ((this.rowCount <= that.rowCount)
        && (this.cpu <= that.cpu)
        && (this.io <= that.io));
  }

  public boolean isLt(RelOptCost other) {
    if (UseCPUCostOnly) {
      CustomCost that = (CustomCost) other;
      return this.cpu < that.cpu;
    }
    return isLe(other) && !equals(other);
  }

  public double getRows() {
    return rowCount;
  }

  @Override public int hashCode() {
    return Objects.hash(rowCount, cpu, io);
  }

  public boolean equals(RelOptCost other) {
    return this == other
        || other instanceof CustomCost
        && (this.rowCount == ((CustomCost) other).rowCount)
        && (this.cpu == ((CustomCost) other).cpu)
        && (this.io == ((CustomCost) other).io);
  }

  @Override public boolean equals(Object obj) {
    if (obj instanceof CustomCost) {
      return equals((CustomCost) obj);
    }
    return false;
  }

  public boolean isEqWithEpsilon(RelOptCost other) {
    if (!(other instanceof CustomCost)) {
      return false;
    }
    CustomCost that = (CustomCost) other;
    return (this == that)
        || ((Math.abs(this.rowCount - that.rowCount) < RelOptUtil.EPSILON)
        && (Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON)
        && (Math.abs(this.io - that.io) < RelOptUtil.EPSILON));
  }

  public RelOptCost minus(RelOptCost other) {
    if (this == INFINITY) {
      return this;
    }
    CustomCost that = (CustomCost) other;
    return new CustomCost(
        this.rowCount - that.rowCount,
        this.cpu - that.cpu,
        this.io - that.io);
  }

  public RelOptCost multiplyBy(double factor) {
    if (this == INFINITY) {
      return this;
    }
    return new CustomCost(rowCount * factor, cpu * factor, io * factor);
  }

  public double divideBy(RelOptCost cost) {
    // Compute the geometric average of the ratios of all of the factors
    // which are non-zero and finite.
    CustomCost that = (CustomCost) cost;
    double d = 1;
    double n = 0;
    if ((this.rowCount != 0)
        && !Double.isInfinite(this.rowCount)
        && (that.rowCount != 0)
        && !Double.isInfinite(that.rowCount)) {
      d *= this.rowCount / that.rowCount;
      ++n;
    }
    if ((this.cpu != 0)
        && !Double.isInfinite(this.cpu)
        && (that.cpu != 0)
        && !Double.isInfinite(that.cpu)) {
      d *= this.cpu / that.cpu;
      ++n;
    }
    if ((this.io != 0)
        && !Double.isInfinite(this.io)
        && (that.io != 0)
        && !Double.isInfinite(that.io)) {
      d *= this.io / that.io;
      ++n;
    }
    if (n == 0) {
      return 1.0;
    }
    return Math.pow(d, 1 / n);
  }

  public RelOptCost plus(RelOptCost other) {
    CustomCost that = (CustomCost) other;
    if ((this == INFINITY) || (that == INFINITY)) {
      return INFINITY;
    }
    return new CustomCost(
        this.rowCount + that.rowCount,
        this.cpu + that.cpu,
        this.io + that.io);
  }

  public String toString() {
    return "{" + rowCount + " rows, " + cpu + " cpu, " + io + " io}";
  }

  /** Implementation of {@link org.apache.calcite.plan.RelOptCostFactory}
   * that creates {@link org.apache.calcite.plan.volcano.CustomCost}s. */
  private static class Factory implements RelOptCostFactory {
    public RelOptCost makeCost(double dRows, double dCpu, double dIo) {
      return new CustomCost(dRows, dCpu, dIo);
    }

    public RelOptCost makeHugeCost() {
      return CustomCost.HUGE;
    }

    public RelOptCost makeInfiniteCost() {
      return CustomCost.INFINITY;
    }

    public RelOptCost makeTinyCost() {
      return CustomCost.TINY;
    }

    public RelOptCost makeZeroCost() {
      return CustomCost.ZERO;
    }
  }
}
