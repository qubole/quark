package com.qubole.quark.planner;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Created by amoghm on 2/9/16.
 */
public class QuarkCubeTest {

  public Fixture fixture = new Fixture();

  private class Fixture {

    QuarkCube.Dimension simple;
    QuarkCube.Dimension mandatory;
    QuarkCube.Dimension hier1;
    QuarkCube.Dimension hier2;
    QuarkCube.Dimension hier3;

    Fixture() {
      simple = QuarkCube.Dimension.builder("I_ITEM_ID", "", "I", "I_ITEM_ID",
          "I_ITEM_ID", 0).build();
      mandatory = QuarkCube.Dimension.builder("CD_GENDER", "", "CD", "CD_GENDER",
          "CD_GENDER", 5).setMandatory(true).build();
      hier1 = QuarkCube.Dimension.builder("D_YEAR", "", "DD", "D_YEAR", "D_YEAR", 1).build();
      hier2 = QuarkCube.Dimension.builder("D_QOY", "", "DD", "D_QOY", "D_QOY", 2)
          .setParentDimension(hier1).build();
      hier3 = QuarkCube.Dimension.builder("D_MOY", "", "DD", "D_MOY", "D_MOY", 3)
          .setParentDimension(hier2).build();
      hier1.addChildDimension(hier2);
      hier2.addChildDimension(hier3);
    }

    public ImmutableSet getAllDim() {
      return ImmutableSet.<QuarkCube.Dimension>of(simple, mandatory, hier1, hier2, hier3);
    }

    public ImmutableSet getHeirDim() {
      return ImmutableSet.<QuarkCube.Dimension>of(hier1, hier2, hier3);
    }
  }

  @Test
  public void computeDimensionSet() {
    Set<Set<QuarkCube.Dimension>> dimSet = QuarkCube.getDimensionSets(
        fixture.getAllDim());
    assertEquals(dimSet.size(), 8);
  }

  @Test
  public void computeHeirDimensionSet() {
    Set<Set<QuarkCube.Dimension>> dimSet = QuarkCube.getDimensionSets(
        fixture.getHeirDim());
    assertEquals(dimSet.size(), 4);
  }

  @Test
  public void computeHeirDimensionSetWithMandatory() {
    QuarkCube.Dimension hier1 =
        QuarkCube.Dimension.builder("D_YEAR", "", "DD", "D_YEAR", "D_YEAR", 1)
            .build();
    QuarkCube.Dimension hier2 =
        QuarkCube.Dimension.builder("D_QOY", "", "DD", "D_QOY", "D_QOY", 2)
            .setParentDimension(hier1).build();
    QuarkCube.Dimension hier3 =
        QuarkCube.Dimension.builder("D_MOY", "", "DD", "D_MOY", "D_MOY", 3)
            .setParentDimension(hier2).setMandatory(true).build();
    hier1.addChildDimension(hier2);
    hier2.addChildDimension(hier3);

    Set<Set<QuarkCube.Dimension>> dimSet = QuarkCube.getDimensionSets(
        ImmutableSet.<QuarkCube.Dimension>of(hier1, hier2, hier3));
    assertEquals(dimSet.size(), 1);
  }

  @Test
  public void computeHeirDimensionSetWithMandatory1() {
    QuarkCube.Dimension hier1 =
        QuarkCube.Dimension.builder("D_YEAR", "", "DD", "D_YEAR", "D_YEAR", 1)
            .build();
    QuarkCube.Dimension hier2 =
        QuarkCube.Dimension.builder("D_QOY", "", "DD", "D_QOY", "D_QOY", 2)
            .setParentDimension(hier1).setMandatory(true).build();
    QuarkCube.Dimension hier3 =
        QuarkCube.Dimension.builder("D_MOY", "", "DD", "D_MOY", "D_MOY", 3)
            .setParentDimension(hier2).build();
    hier1.addChildDimension(hier2);
    hier2.addChildDimension(hier3);

    Set<Set<QuarkCube.Dimension>> dimSet = QuarkCube.getDimensionSets(
        ImmutableSet.<QuarkCube.Dimension>of(hier1, hier2, hier3));
    assertEquals(dimSet.size(), 2);
  }

}
