package de.hpi.julianweise.slave.query.join.cost.calculators;

import de.hpi.julianweise.slave.query.join.cost.interval.ADBInterval;
import de.hpi.julianweise.slave.query.join.cost.interval.ADBIntervalImpl;
import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.Test;

import static de.hpi.julianweise.slave.query.join.cost.interval.ADBIntervalImpl.NO_INTERSECTION;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("unchecked")
public class ADBJoinTermGreaterOrEqualCostCalculatorTest {

    @Test
    public void expectEmptyArrayForEmptyLists() {
        ADBJoinTermGreaterOrEqualCostCalculator calculator = new ADBJoinTermGreaterOrEqualCostCalculator();

        ObjectList<ADBComparable2IntPair> left = new ObjectArrayList<>();
        ObjectList<ADBComparable2IntPair> right = new ObjectArrayList<>();

        ADBInterval[] result = calculator.calc(left, right);
        assertThat(result.length).isZero();
    }

    @Test
    public void expectEmptyArrayForEmptyLeftLists() {
        ADBJoinTermGreaterOrEqualCostCalculator calculator = new ADBJoinTermGreaterOrEqualCostCalculator();

        ObjectList<ADBComparable2IntPair> left = new ObjectArrayList<>();
        ObjectList<ADBComparable2IntPair> right = new ObjectArrayList<>();
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)1, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)2, 2));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)3, 3));

        ADBInterval[] result = calculator.calc(left, right);
        assertThat(result.length).isZero();
    }

    @Test
    public void expectMoIntersectsForEmptyRightLists() {
        ADBJoinTermGreaterOrEqualCostCalculator calculator = new ADBJoinTermGreaterOrEqualCostCalculator();

        ObjectList<ADBComparable2IntPair> left = new ObjectArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)1, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)2, 2));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)3, 3));
        ObjectList<ADBComparable2IntPair> right = new ObjectArrayList<>();

        ADBInterval[] result = calculator.calc(left, right);
        assertThat(result.length).isEqualTo(3);
        assertThat(result[0]).isEqualTo(NO_INTERSECTION);
        assertThat(result[1]).isEqualTo(NO_INTERSECTION);
        assertThat(result[2]).isEqualTo(NO_INTERSECTION);
    }

    @Test
    public void expectValidResultsForBothListsFilled() {
        ADBJoinTermGreaterOrEqualCostCalculator calculator = new ADBJoinTermGreaterOrEqualCostCalculator();

        ObjectList<ADBComparable2IntPair> left = new ObjectArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)0, 0));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)1, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)2, 2));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)3, 3));
        ObjectList<ADBComparable2IntPair> right = new ObjectArrayList<>();
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)1, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)2, 2));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)2, 3));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)3, 4));

        ADBInterval[] result = calculator.calc(left, right);
        assertThat(result.length).isEqualTo(4);
        assertThat(result[0]).isEqualTo(NO_INTERSECTION);
        assertThat(result[1]).isEqualTo(new ADBIntervalImpl(0, 0));
        assertThat(result[2]).isEqualTo(new ADBIntervalImpl(0, 2));
        assertThat(result[3]).isEqualTo(new ADBIntervalImpl(0, 3));
    }

}