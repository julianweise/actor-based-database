package de.hpi.julianweise.utility;


import de.hpi.julianweise.utility.largemessage.ADBComparable2IntPair;
import de.hpi.julianweise.utility.query.join.ADBOffsetCalculator;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("unchecked")
public class ADBOffsetCalculatorTest {

    @Test
    public void expectCorrectResultForEquallyLongSetWithDirectMatches() {
        ObjectList<ADBComparable2IntPair> left = new ObjectArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)5, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)22, 1));
        ObjectList<ADBComparable2IntPair> right = new ObjectArrayList<>();
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
        ObjectList<ADBComparable2IntPair> left = new ObjectArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)5, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)22, 1));
        ObjectList<ADBComparable2IntPair> right = new ObjectArrayList<>();
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
        ObjectList<ADBComparable2IntPair> left = new ObjectArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)5, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)22, 1));
        ObjectList<ADBComparable2IntPair> right = new ObjectArrayList<>();
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
        ObjectList<ADBComparable2IntPair> left = new ObjectArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        ObjectList<ADBComparable2IntPair> right = new ObjectArrayList<>();
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
        ObjectList<ADBComparable2IntPair> left = new ObjectArrayList<>();
        ObjectList<ADBComparable2IntPair> right = new ObjectArrayList<>();
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)1, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)14, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)16, 1));
        right.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)17, 1));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(0);
    }

    @Test
    public void expectCorrectResultRightEmpty() {
        ObjectList<ADBComparable2IntPair> left = new ObjectArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)5, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)22, 1));
        ObjectList<ADBComparable2IntPair> right = new ObjectArrayList<>();

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(-1);
        assertThat(leftToRightOffset[1]).isEqualTo(-1);
        assertThat(leftToRightOffset[2]).isEqualTo(-1);
        assertThat(leftToRightOffset[3]).isEqualTo(-1);
    }

    @Test
    public void expectCorrectResultForEquallyLongSetWithDuplicatesRight() {
        ObjectList<ADBComparable2IntPair> left = new ObjectArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)5, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)22, 1));
        ObjectList<ADBComparable2IntPair> right = new ObjectArrayList<>();
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
        ObjectList<ADBComparable2IntPair> left = new ObjectArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)12, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)12, 1));
        ObjectList<ADBComparable2IntPair> right = new ObjectArrayList<>();
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
        ObjectList<ADBComparable2IntPair> left = new ObjectArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)12, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)12, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        ObjectList<ADBComparable2IntPair> right = new ObjectArrayList<>();
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
        ObjectList<ADBComparable2IntPair> left = new ObjectArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)5, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)22, 1));
        ObjectList<ADBComparable2IntPair> right = new ObjectArrayList<>();
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
        ObjectList<ADBComparable2IntPair> left = new ObjectArrayList<>();
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)5, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)8, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)13, 1));
        left.add(new ADBComparable2IntPair((Comparable<Object>)(Comparable<?>)22, 1));
        ObjectList<ADBComparable2IntPair> right = new ObjectArrayList<>();
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