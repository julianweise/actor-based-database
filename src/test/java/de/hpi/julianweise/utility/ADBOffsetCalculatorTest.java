package de.hpi.julianweise.utility;


import de.hpi.julianweise.utility.largemessage.ADBPair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBOffsetCalculatorTest {

    @Test
    public void expectCorrectResultForEquallyLongSetWithDirectMatches() {
        List<ADBPair<Comparable<?>, Integer>> left = new ArrayList<>();
        left.add(new ADBPair<>(5, 1));
        left.add(new ADBPair<>(8, 1));
        left.add(new ADBPair<>(13, 1));
        left.add(new ADBPair<>(22, 1));
        List<ADBPair<Comparable<?>, Integer>> right = new ArrayList<>();
        right.add(new ADBPair<>(1, 1));
        right.add(new ADBPair<>(13, 1));
        right.add(new ADBPair<>(16, 1));
        right.add(new ADBPair<>(22, 1));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(1);
        assertThat(leftToRightOffset[1]).isEqualTo(1);
        assertThat(leftToRightOffset[2]).isEqualTo(1);
        assertThat(leftToRightOffset[3]).isEqualTo(3);
    }

    @Test
    public void expectCorrectResultForEquallyLongSetsWithoutDirectMatches() {
        List<ADBPair<Comparable<?>, Integer>> left = new ArrayList<>();
        left.add(new ADBPair<>(5, 1));
        left.add(new ADBPair<>(8, 1));
        left.add(new ADBPair<>(13, 1));
        left.add(new ADBPair<>(22, 1));
        List<ADBPair<Comparable<?>, Integer>> right = new ArrayList<>();
        right.add(new ADBPair<>(1, 1));
        right.add(new ADBPair<>(14, 1));
        right.add(new ADBPair<>(16, 1));
        right.add(new ADBPair<>(17, 1));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(1);
        assertThat(leftToRightOffset[1]).isEqualTo(1);
        assertThat(leftToRightOffset[2]).isEqualTo(1);
        assertThat(leftToRightOffset[3]).isEqualTo(3);
    }

    @Test
    public void expectCorrectResultSkewedSetsRight() {
        List<ADBPair<Comparable<?>, Integer>> left = new ArrayList<>();
        left.add(new ADBPair<>(5, 1));
        left.add(new ADBPair<>(8, 1));
        left.add(new ADBPair<>(13, 1));
        left.add(new ADBPair<>(22, 1));
        List<ADBPair<Comparable<?>, Integer>> right = new ArrayList<>();
        right.add(new ADBPair<>(17, 1));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(0);
        assertThat(leftToRightOffset[1]).isEqualTo(0);
        assertThat(leftToRightOffset[2]).isEqualTo(0);
        assertThat(leftToRightOffset[3]).isEqualTo(0);
    }

    @Test
    public void expectCorrectResultSkewedSetsLeft() {
        List<ADBPair<Comparable<?>, Integer>> left = new ArrayList<>();
        left.add(new ADBPair<>(13, 1));
        List<ADBPair<Comparable<?>, Integer>> right = new ArrayList<>();
        right.add(new ADBPair<>(1, 1));
        right.add(new ADBPair<>(14, 1));
        right.add(new ADBPair<>(16, 1));
        right.add(new ADBPair<>(17, 1));


        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(1);
        assertThat(leftToRightOffset[0]).isEqualTo(1);
    }

    @Test
    public void expectCorrectResultLeftEmpty() {
        List<ADBPair<Comparable<?>, Integer>> left = new ArrayList<>();
        List<ADBPair<Comparable<?>, Integer>> right = new ArrayList<>();
        right.add(new ADBPair<>(1, 1));
        right.add(new ADBPair<>(14, 1));
        right.add(new ADBPair<>(16, 1));
        right.add(new ADBPair<>(17, 1));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(0);
    }

    @Test
    public void expectCorrectResultRightEmpty() {
        List<ADBPair<Comparable<?>, Integer>> left = new ArrayList<>();
        left.add(new ADBPair<>(5, 1));
        left.add(new ADBPair<>(8, 1));
        left.add(new ADBPair<>(13, 1));
        left.add(new ADBPair<>(22, 1));
        List<ADBPair<Comparable<?>, Integer>> right = new ArrayList<>();

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(-1);
        assertThat(leftToRightOffset[1]).isEqualTo(-1);
        assertThat(leftToRightOffset[2]).isEqualTo(-1);
        assertThat(leftToRightOffset[3]).isEqualTo(-1);
    }

    @Test
    public void expectCorrectResultForEquallyLongSetWithDuplicatesRight() {
        List<ADBPair<Comparable<?>, Integer>> left = new ArrayList<>();
        left.add(new ADBPair<>(5, 1));
        left.add(new ADBPair<>(8, 1));
        left.add(new ADBPair<>(13, 1));
        left.add(new ADBPair<>(22, 1));
        List<ADBPair<Comparable<?>, Integer>> right = new ArrayList<>();
        right.add(new ADBPair<>(1, 1));
        right.add(new ADBPair<>(13, 1));
        right.add(new ADBPair<>(13, 1));
        right.add(new ADBPair<>(16, 1));
        right.add(new ADBPair<>(22, 1));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(1);
        assertThat(leftToRightOffset[1]).isEqualTo(1);
        assertThat(leftToRightOffset[2]).isEqualTo(2);
        assertThat(leftToRightOffset[3]).isEqualTo(4);
    }

    @Test
    public void expectCorrectResultForEquallyLongSetWithDuplicatesLeft() {
        List<ADBPair<Comparable<?>, Integer>> left = new ArrayList<>();
        left.add(new ADBPair<>(8, 1));
        left.add(new ADBPair<>(8, 1));
        left.add(new ADBPair<>(12, 1));
        left.add(new ADBPair<>(12, 1));
        List<ADBPair<Comparable<?>, Integer>> right = new ArrayList<>();
        right.add(new ADBPair<>(1, 1));
        right.add(new ADBPair<>(8, 1));
        right.add(new ADBPair<>(13, 1));
        right.add(new ADBPair<>(16, 1));
        right.add(new ADBPair<>(22, 1));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(1);
        assertThat(leftToRightOffset[1]).isEqualTo(1);
        assertThat(leftToRightOffset[2]).isEqualTo(2);
        assertThat(leftToRightOffset[3]).isEqualTo(2);
    }

    @Test
    public void expectCorrectResultForEquallyLongSetWithDuplicatesBoth() {
        List<ADBPair<Comparable<?>, Integer>> left = new ArrayList<>();
        left.add(new ADBPair<>(8, 1));
        left.add(new ADBPair<>(8, 1));
        left.add(new ADBPair<>(12, 1));
        left.add(new ADBPair<>(12, 1));
        left.add(new ADBPair<>(13, 1));
        left.add(new ADBPair<>(13, 1));
        List<ADBPair<Comparable<?>, Integer>> right = new ArrayList<>();
        right.add(new ADBPair<>(1, 1));
        right.add(new ADBPair<>(8, 1));
        right.add(new ADBPair<>(8, 1));
        right.add(new ADBPair<>(13, 1));
        right.add(new ADBPair<>(13, 1));

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
        List<ADBPair<Comparable<?>, Integer>> left = new ArrayList<>();
        left.add(new ADBPair<>(5, 1));
        left.add(new ADBPair<>(8, 1));
        left.add(new ADBPair<>(13, 1));
        left.add(new ADBPair<>(22, 1));
        List<ADBPair<Comparable<?>, Integer>> right = new ArrayList<>();
        right.add(new ADBPair<>(1, 1));
        right.add(new ADBPair<>(2, 1));
        right.add(new ADBPair<>(3, 1));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(2);
        assertThat(leftToRightOffset[1]).isEqualTo(2);
        assertThat(leftToRightOffset[2]).isEqualTo(2);
        assertThat(leftToRightOffset[3]).isEqualTo(2);
    }

    @Test
    public void expectCorrectResultsForManyItemsLargerSmaller() {
        List<ADBPair<Comparable<?>, Integer>> left = new ArrayList<>();
        left.add(new ADBPair<>(5, 1));
        left.add(new ADBPair<>(8, 1));
        left.add(new ADBPair<>(13, 1));
        left.add(new ADBPair<>(22, 1));
        List<ADBPair<Comparable<?>, Integer>> right = new ArrayList<>();
        right.add(new ADBPair<>(22, 1));
        right.add(new ADBPair<>(24, 1));
        right.add(new ADBPair<>(33, 1));

        int[] leftToRightOffset = ADBOffsetCalculator.calc(left, right);

        assertThat(leftToRightOffset.length).isEqualTo(4);
        assertThat(leftToRightOffset[0]).isEqualTo(0);
        assertThat(leftToRightOffset[1]).isEqualTo(0);
        assertThat(leftToRightOffset[2]).isEqualTo(0);
        assertThat(leftToRightOffset[3]).isEqualTo(0);
    }

}