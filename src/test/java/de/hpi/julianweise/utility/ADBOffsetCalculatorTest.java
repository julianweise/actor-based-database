package de.hpi.julianweise.utility;


import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import de.hpi.julianweise.utility.query.join.ADBOffsetCalculator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("unchecked")
public class ADBOffsetCalculatorTest {

    @Test
    public void expectCorrectResultForEquallyLongSetWithDirectMatches() {
        List<ADBComparable2IntPair> left = new ArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)5, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)22, 1));
        List<ADBComparable2IntPair> right = new ArrayList<>();
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)1, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)16, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)22, 1));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(1);
        assertThat(leftToRightOffset[1]).isEqualTo(1);
        assertThat(leftToRightOffset[2]).isEqualTo(1);
        assertThat(leftToRightOffset[3]).isEqualTo(3);
    }

    @Test
    public void expectCorrectResultForEquallyLongSetsWithoutDirectMatches() {
        List<ADBComparable2IntPair> left = new ArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)5, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)22, 1));
        List<ADBComparable2IntPair> right = new ArrayList<>();
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)1, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)14, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)16, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)17, 1));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(1);
        assertThat(leftToRightOffset[1]).isEqualTo(1);
        assertThat(leftToRightOffset[2]).isEqualTo(1);
        assertThat(leftToRightOffset[3]).isEqualTo(3);
    }

    @Test
    public void expectCorrectResultSkewedSetsRight() {
        List<ADBComparable2IntPair> left = new ArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)5, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)22, 1));
        List<ADBComparable2IntPair> right = new ArrayList<>();
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)17, 1));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(0);
        assertThat(leftToRightOffset[1]).isEqualTo(0);
        assertThat(leftToRightOffset[2]).isEqualTo(0);
        assertThat(leftToRightOffset[3]).isEqualTo(0);
    }

    @Test
    public void expectCorrectResultSkewedSetsLeft() {
        List<ADBComparable2IntPair> left = new ArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        List<ADBComparable2IntPair> right = new ArrayList<>();
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)1, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)14, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)16, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)17, 1));


        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(1);
        assertThat(leftToRightOffset[0]).isEqualTo(1);
    }

    @Test
    public void expectCorrectResultLeftEmpty() {
        List<ADBComparable2IntPair> left = new ArrayList<>();
        List<ADBComparable2IntPair> right = new ArrayList<>();
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)1, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)14, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)16, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)17, 1));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(0);
    }

    @Test
    public void expectCorrectResultRightEmpty() {
        List<ADBComparable2IntPair> left = new ArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)5, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)22, 1));
        List<ADBComparable2IntPair> right = new ArrayList<>();

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(-1);
        assertThat(leftToRightOffset[1]).isEqualTo(-1);
        assertThat(leftToRightOffset[2]).isEqualTo(-1);
        assertThat(leftToRightOffset[3]).isEqualTo(-1);
    }

    @Test
    public void expectCorrectResultForEquallyLongSetWithDuplicatesRight() {
        List<ADBComparable2IntPair> left = new ArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)5, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)22, 1));
        List<ADBComparable2IntPair> right = new ArrayList<>();
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)1, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)16, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)22, 1));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(1);
        assertThat(leftToRightOffset[1]).isEqualTo(1);
        assertThat(leftToRightOffset[2]).isEqualTo(2);
        assertThat(leftToRightOffset[3]).isEqualTo(4);
    }

    @Test
    public void expectCorrectResultForEquallyLongSetWithDuplicatesLeft() {
        List<ADBComparable2IntPair> left = new ArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)12, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)12, 1));
        List<ADBComparable2IntPair> right = new ArrayList<>();
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)1, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)16, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)22, 1));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(1);
        assertThat(leftToRightOffset[1]).isEqualTo(1);
        assertThat(leftToRightOffset[2]).isEqualTo(2);
        assertThat(leftToRightOffset[3]).isEqualTo(2);
    }

    @Test
    public void expectCorrectResultForEquallyLongSetWithDuplicatesBoth() {
        List<ADBComparable2IntPair> left = new ArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)12, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)12, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        List<ADBComparable2IntPair> right = new ArrayList<>();
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)1, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(6);
        assertThat(leftToRightOffset[0]).isEqualTo(2);
        assertThat(leftToRightOffset[1]).isEqualTo(2);
        assertThat(leftToRightOffset[2]).isEqualTo(3);
        assertThat(leftToRightOffset[3]).isEqualTo(3);
        assertThat(leftToRightOffset[4]).isEqualTo(4);
        assertThat(leftToRightOffset[5]).isEqualTo(4);
    }


    @Test
    public void expectCorrectResultsForManyItemsLarger() {
        List<ADBComparable2IntPair> left = new ArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)5, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)22, 1));
        List<ADBComparable2IntPair> right = new ArrayList<>();
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)1, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)2, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)3, 1));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(2);
        assertThat(leftToRightOffset[1]).isEqualTo(2);
        assertThat(leftToRightOffset[2]).isEqualTo(2);
        assertThat(leftToRightOffset[3]).isEqualTo(2);
    }

    @Test
    public void expectCorrectResultsForManyItemsLargerSmaller() {
        List<ADBComparable2IntPair> left = new ArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)5, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)22, 1));
        List<ADBComparable2IntPair> right = new ArrayList<>();
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)22, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)24, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)33, 1));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(0);
        assertThat(leftToRightOffset[1]).isEqualTo(0);
        assertThat(leftToRightOffset[2]).isEqualTo(0);
        assertThat(leftToRightOffset[3]).isEqualTo(0);
    }

}