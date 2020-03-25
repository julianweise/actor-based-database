package de.hpi.julianweise.shard.queryOperation.join;

import javafx.util.Pair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ADBJoinAttributeIntersectorTest {

    @Test
    public void intersectEmptyLists() {
        List<Pair<Integer, Integer>> setA = new ArrayList<>();
        List<Pair<Integer, Integer>> setB = new ArrayList<>();

        assertThat(ADBJoinAttributeIntersector.intersect(setA, setB).size()).isZero();
    }

    @Test
    public void intersectWithOneEmptyList() {
        List<Pair<Integer, Integer>> setA = new ArrayList<>(5);
        setA.add(new Pair<>(1,2));
        setA.add(new Pair<>(2,3));
        setA.add(new Pair<>(3,4));
        setA.add(new Pair<>(4,5));
        setA.add(new Pair<>(5,6));
        List<Pair<Integer, Integer>> setB = new ArrayList<>();

        assertThat(ADBJoinAttributeIntersector.intersect(setA, setB).size()).isZero();
    }

    @Test
    public void intersectWithOtherEmptyList() {
        List<Pair<Integer, Integer>> setB = new ArrayList<>(5);
        setB.add(new Pair<>(1,2));
        setB.add(new Pair<>(2,3));
        setB.add(new Pair<>(3,4));
        setB.add(new Pair<>(4,5));
        setB.add(new Pair<>(5,6));
        List<Pair<Integer, Integer>> setA = new ArrayList<>();

        assertThat(ADBJoinAttributeIntersector.intersect(setA, setB).size()).isZero();
    }

    @Test
    public void intersectTwoFilledListsCorrectly() {
        List<Pair<Integer, Integer>> setA = new ArrayList<>(5);
        setA.add(new Pair<>(1,2));
        setA.add(new Pair<>(2,3));
        setA.add(new Pair<>(3,4));
        setA.add(new Pair<>(4,5));
        setA.add(new Pair<>(5,6));
        List<Pair<Integer, Integer>> setB = new ArrayList<>();
        setB.add(new Pair<>(1,2));
        setB.add(new Pair<>(3,3));
        setB.add(new Pair<>(6,4));
        setB.add(new Pair<>(4,5));
        setB.add(new Pair<>(9,6));

        assertThat(ADBJoinAttributeIntersector.intersect(setA, setB).size()).isEqualTo(2);
    }

    @Test
    public void intersectTwoFilledListsCorrectlyRemoveNotIntersectedDuplicates() {
        List<Pair<Integer, Integer>> setA = new ArrayList<>(5);
        setA.add(new Pair<>(1,2));
        setA.add(new Pair<>(2,3));
        setA.add(new Pair<>(2,3));
        setA.add(new Pair<>(3,4));
        setA.add(new Pair<>(4,5));
        setA.add(new Pair<>(5,6));
        List<Pair<Integer, Integer>> setB = new ArrayList<>();
        setB.add(new Pair<>(1,2));
        setB.add(new Pair<>(3,3));
        setB.add(new Pair<>(6,4));
        setB.add(new Pair<>(4,5));
        setB.add(new Pair<>(9,6));
        setB.add(new Pair<>(9,6));

        assertThat(ADBJoinAttributeIntersector.intersect(setA, setB).size()).isEqualTo(2);
    }

    @Test
    public void intersectTwoFilledListsCorrectlyRemoveIntersectedDuplicates() {
        List<Pair<Integer, Integer>> setA = new ArrayList<>(5);
        setA.add(new Pair<>(1,2));
        setA.add(new Pair<>(2,3));
        setA.add(new Pair<>(3,4));
        setA.add(new Pair<>(4,5));
        setA.add(new Pair<>(5,6));
        setA.add(new Pair<>(4,5));
        setA.add(new Pair<>(1,2));
        List<Pair<Integer, Integer>> setB = new ArrayList<>();
        setB.add(new Pair<>(1,2));
        setB.add(new Pair<>(3,3));
        setB.add(new Pair<>(6,4));
        setB.add(new Pair<>(4,5));
        setB.add(new Pair<>(9,6));
        setB.add(new Pair<>(4,5));
        setB.add(new Pair<>(1,2));

        assertThat(ADBJoinAttributeIntersector.intersect(setA, setB).size()).isEqualTo(2);
    }

}