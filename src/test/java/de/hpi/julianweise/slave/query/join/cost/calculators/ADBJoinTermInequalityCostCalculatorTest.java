package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBInverseInterval;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("unchecked")
public class ADBJoinTermInequalityCostCalculatorTest {

    @Test
    public void expectEmptyArrayForEmptyLists() {
        ADBJoinTermInequalityCostCalculator calculator = new ADBJoinTermInequalityCostCalculator();

        List<ADBPair<Comparable<Object>, Integer>> left = new ArrayList<>();
        List<ADBPair<Comparable<Object>, Integer>> right = new ArrayList<>();

        ADBInterval[] result = calculator.calc(left, right);
        assertThat(result.length).isZero();
    }

    @Test
    public void expectEmptyArrayForEmptyLeftLists() {
        ADBJoinTermInequalityCostCalculator calculator = new ADBJoinTermInequalityCostCalculator();

        List<ADBPair<Comparable<Object>, Integer>> left = new ArrayList<>();
        List<ADBPair<Comparable<Object>, Integer>> right = new ArrayList<>();
        right.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)1, 1));
        right.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)2, 2));
        right.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)3, 3));

        ADBInterval[] result = calculator.calc(left, right);
        assertThat(result.length).isZero();
    }

    @Test
    public void expectMoIntersectsForEmptyRightLists() {
        ADBJoinTermInequalityCostCalculator calculator = new ADBJoinTermInequalityCostCalculator();

        List<ADBPair<Comparable<Object>, Integer>> left = new ArrayList<>();
        left.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)1, 1));
        left.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)2, 2));
        left.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)3, 3));
        List<ADBPair<Comparable<Object>, Integer>> right = new ArrayList<>();

        ADBInterval[] result = calculator.calc(left, right);
        assertThat(result.length).isEqualTo(3);
        assertThat(result[0]).isEqualTo(ADBInverseInterval.NO_INTERSECTION);
        assertThat(result[1]).isEqualTo(ADBInverseInterval.NO_INTERSECTION);
        assertThat(result[2]).isEqualTo(ADBInverseInterval.NO_INTERSECTION);
    }

    @Test
    public void expectValidResultsForBothListsFilled() {
        ADBJoinTermInequalityCostCalculator calculator = new ADBJoinTermInequalityCostCalculator();

        List<ADBPair<Comparable<Object>, Integer>> left = new ArrayList<>();
        left.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)0, 0));
        left.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)1, 1));
        left.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)2, 2));
        left.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)3, 3));
        List<ADBPair<Comparable<Object>, Integer>> right = new ArrayList<>();
        right.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)1, 1));
        right.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)2, 2));
        right.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)2, 3));
        right.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)3, 4));

        ADBInterval[] result = calculator.calc(left, right);
        assertThat(result.length).isEqualTo(4);
        assertThat(result[0]).isEqualTo(new ADBInverseInterval(-1, -1, 3));
        assertThat(result[1]).isEqualTo(new ADBInverseInterval(0, 0, 3));
        assertThat(result[2]).isEqualTo(new ADBInverseInterval(1, 2, 3));
        assertThat(result[3]).isEqualTo(new ADBInverseInterval(3, 3, 3));
    }

}