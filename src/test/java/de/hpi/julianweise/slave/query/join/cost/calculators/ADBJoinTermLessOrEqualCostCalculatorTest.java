package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBIntervalImpl;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@SuppressWarnings("unchecked")
public class ADBJoinTermLessOrEqualCostCalculatorTest {

    @Test
    public void expectEmptyArrayForEmptyLists() {
        ADBJoinTermLessOrEqualCostCalculator calculator = new ADBJoinTermLessOrEqualCostCalculator();

        List<ADBPair<Comparable<Object>, Integer>> left = new ArrayList<>();
        List<ADBPair<Comparable<Object>, Integer>> right = new ArrayList<>();

        ADBInterval[] result = calculator.calc(left, right);
        assertThat(result.length).isZero();
    }

    @Test
    public void expectEmptyArrayForEmptyLeftLists() {
        ADBJoinTermLessOrEqualCostCalculator calculator = new ADBJoinTermLessOrEqualCostCalculator();

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
        ADBJoinTermLessOrEqualCostCalculator calculator = new ADBJoinTermLessOrEqualCostCalculator();

        List<ADBPair<Comparable<Object>, Integer>> left = new ArrayList<>();
        left.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)1, 1));
        left.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)2, 2));
        left.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)3, 3));
        List<ADBPair<Comparable<Object>, Integer>> right = new ArrayList<>();

        ADBInterval[] result = calculator.calc(left, right);
        assertThat(result.length).isEqualTo(3);
        assertThat(result[0]).isEqualTo(ADBIntervalImpl.NO_INTERSECTION);
        assertThat(result[1]).isEqualTo(ADBIntervalImpl.NO_INTERSECTION);
        assertThat(result[2]).isEqualTo(ADBIntervalImpl.NO_INTERSECTION);
    }

    @Test
    public void expectValidResultsForBothListsFilled() {
        ADBJoinTermLessOrEqualCostCalculator calculator = new ADBJoinTermLessOrEqualCostCalculator();

        List<ADBPair<Comparable<Object>, Integer>> left = new ArrayList<>();
        left.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)0, 0));
        left.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)1, 1));
        left.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)2, 2));
        left.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)3, 3));
        List<ADBPair<Comparable<Object>, Integer>> right = new ArrayList<>();
        right.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)1, 1));
        right.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)2, 2));
        right.add(new ADBPair<>((Comparable<Object>)(Comparable<?>)2, 3));

        ADBInterval[] result = calculator.calc(left, right);
        assertThat(result.length).isEqualTo(4);
        assertThat(result[0]).isEqualTo(new ADBIntervalImpl(0, 2));
        assertThat(result[1]).isEqualTo(new ADBIntervalImpl(0, 2));
        assertThat(result[2]).isEqualTo(new ADBIntervalImpl(1, 2));
        assertThat(result[3]).isEqualTo(ADBIntervalImpl.NO_INTERSECTION);
    }

}