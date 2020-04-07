package de.hpi.julianweise.shard.query_operation.join;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ADBJoinAttributeIntersectorTest {

    @ClassRule
    public static TestKitJunitResource testKit = new TestKitJunitResource();


    @After
    public void cleanup() {
        testKit.after();
        testKit = new TestKitJunitResource();
    }

    @AfterClass
    public static void after() {
        testKit.after();
    }

    @Test
    public void intersectEmptyLists() {
        List<ADBKeyPair> setA = new ArrayList<>();
        List<ADBKeyPair> setB = new ArrayList<>();
        TestProbe<ADBJoinAttributeIntersector.Result> resultTestProbe = testKit.createTestProbe();

        ActorRef<ADBJoinAttributeIntersector.Command> intersector = testKit.spawn(
                ADBJoinAttributeIntersectorFactory.createDefault(setA));

        intersector.tell(new ADBJoinAttributeIntersector.Intersect(setB));
        intersector.tell(new ADBJoinAttributeIntersector.ReturnResults(resultTestProbe.ref()));

        ADBJoinAttributeIntersector.Results result =
                resultTestProbe.expectMessageClass(ADBJoinAttributeIntersector.Results.class);


        assertThat(result.getCandidates().size()).isZero();
    }

    @Test
    public void intersectWithOneEmptyList() {
        List<ADBKeyPair> setA = new ArrayList<>(5);
        setA.add(new ADBKeyPair(1,2));
        setA.add(new ADBKeyPair(2,3));
        setA.add(new ADBKeyPair(3,4));
        setA.add(new ADBKeyPair(4,5));
        setA.add(new ADBKeyPair(5,6));
        List<ADBKeyPair> setB = new ArrayList<>();

        TestProbe<ADBJoinAttributeIntersector.Result> resultTestProbe = testKit.createTestProbe();

        ActorRef<ADBJoinAttributeIntersector.Command> intersector = testKit.spawn(
                ADBJoinAttributeIntersectorFactory.createDefault(setA));

        intersector.tell(new ADBJoinAttributeIntersector.Intersect(setB));
        intersector.tell(new ADBJoinAttributeIntersector.ReturnResults(resultTestProbe.ref()));

        ADBJoinAttributeIntersector.Results result =
                resultTestProbe.expectMessageClass(ADBJoinAttributeIntersector.Results.class);

        assertThat(result.getCandidates().size()).isZero();
    }

    @Test
    public void intersectWithOtherEmptyList() {
        List<ADBKeyPair> setA = new ArrayList<>(5);
        setA.add(new ADBKeyPair(1,2));
        setA.add(new ADBKeyPair(2,3));
        setA.add(new ADBKeyPair(3,4));
        setA.add(new ADBKeyPair(4,5));
        setA.add(new ADBKeyPair(5,6));
        List<ADBKeyPair> setB = new ArrayList<>();

        TestProbe<ADBJoinAttributeIntersector.Result> resultTestProbe = testKit.createTestProbe();

        ActorRef<ADBJoinAttributeIntersector.Command> intersector = testKit.spawn(
                ADBJoinAttributeIntersectorFactory.createDefault(setB));

        intersector.tell(new ADBJoinAttributeIntersector.Intersect(setA));
        intersector.tell(new ADBJoinAttributeIntersector.ReturnResults(resultTestProbe.ref()));

        ADBJoinAttributeIntersector.Results result =
                resultTestProbe.expectMessageClass(ADBJoinAttributeIntersector.Results.class);

        assertThat(result.getCandidates().size()).isZero();
    }

    @Test
    public void intersectTwoFilledListsCorrectly() {
        List<ADBKeyPair> setA = new ArrayList<>(5);
        setA.add(new ADBKeyPair(1,2));
        setA.add(new ADBKeyPair(2,3));
        setA.add(new ADBKeyPair(3,4));
        setA.add(new ADBKeyPair(4,5));
        setA.add(new ADBKeyPair(5,6));
        List<ADBKeyPair> setB = new ArrayList<>();
        setB.add(new ADBKeyPair(1,2));
        setB.add(new ADBKeyPair(3,3));
        setB.add(new ADBKeyPair(6,4));
        setB.add(new ADBKeyPair(4,5));
        setB.add(new ADBKeyPair(9,6));

        TestProbe<ADBJoinAttributeIntersector.Result> resultTestProbe = testKit.createTestProbe();

        ActorRef<ADBJoinAttributeIntersector.Command> intersector = testKit.spawn(
                ADBJoinAttributeIntersectorFactory.createDefault(setA));

        intersector.tell(new ADBJoinAttributeIntersector.Intersect(setB));
        intersector.tell(new ADBJoinAttributeIntersector.ReturnResults(resultTestProbe.ref()));

        ADBJoinAttributeIntersector.Results result =
                resultTestProbe.expectMessageClass(ADBJoinAttributeIntersector.Results.class);

        assertThat(result.getCandidates().size()).isEqualTo(2);
        assertThat(result.getCandidates().get(0)).isEqualTo(setA.get(0));
        assertThat(result.getCandidates().get(1)).isEqualTo(setA.get(3));
    }

    @Test
    public void intersectTwoFilledListsCorrectlyRemoveNotIntersectedDuplicates() {
        List<ADBKeyPair> setA = new ArrayList<>(5);
        setA.add(new ADBKeyPair(1,2));
        setA.add(new ADBKeyPair(2,3));
        setA.add(new ADBKeyPair(2,3));
        setA.add(new ADBKeyPair(3,4));
        setA.add(new ADBKeyPair(4,5));
        setA.add(new ADBKeyPair(5,6));
        List<ADBKeyPair> setB = new ArrayList<>();
        setB.add(new ADBKeyPair(1,2));
        setB.add(new ADBKeyPair(3,3));
        setB.add(new ADBKeyPair(6,4));
        setB.add(new ADBKeyPair(4,5));
        setB.add(new ADBKeyPair(9,6));
        setB.add(new ADBKeyPair(9,6));

        TestProbe<ADBJoinAttributeIntersector.Result> resultTestProbe = testKit.createTestProbe();

        ActorRef<ADBJoinAttributeIntersector.Command> intersector = testKit.spawn(
                ADBJoinAttributeIntersectorFactory.createDefault(setA));

        intersector.tell(new ADBJoinAttributeIntersector.Intersect(setB));
        intersector.tell(new ADBJoinAttributeIntersector.ReturnResults(resultTestProbe.ref()));

        ADBJoinAttributeIntersector.Results result =
                resultTestProbe.expectMessageClass(ADBJoinAttributeIntersector.Results.class);

        assertThat(result.getCandidates().size()).isEqualTo(2);
        assertThat(result.getCandidates().get(0)).isEqualTo(setA.get(0));
        assertThat(result.getCandidates().get(1)).isEqualTo(setA.get(4));
    }

    @Test
    public void intersectTwoFilledListsCorrectlyRemoveIntersectedDuplicates() {
        List<ADBKeyPair> setA = new ArrayList<>(5);
        setA.add(new ADBKeyPair(1,2));
        setA.add(new ADBKeyPair(2,3));
        setA.add(new ADBKeyPair(3,4));
        setA.add(new ADBKeyPair(4,5));
        setA.add(new ADBKeyPair(5,6));
        setA.add(new ADBKeyPair(4,5));
        setA.add(new ADBKeyPair(1,2));
        List<ADBKeyPair> setB = new ArrayList<>();
        setB.add(new ADBKeyPair(1,2));
        setB.add(new ADBKeyPair(3,3));
        setB.add(new ADBKeyPair(6,4));
        setB.add(new ADBKeyPair(4,5));
        setB.add(new ADBKeyPair(9,6));
        setB.add(new ADBKeyPair(4,5));
        setB.add(new ADBKeyPair(1,2));

        TestProbe<ADBJoinAttributeIntersector.Result> resultTestProbe = testKit.createTestProbe();

        ActorRef<ADBJoinAttributeIntersector.Command> intersector = testKit.spawn(
                ADBJoinAttributeIntersectorFactory.createDefault(setA));

        intersector.tell(new ADBJoinAttributeIntersector.Intersect(setB));
        intersector.tell(new ADBJoinAttributeIntersector.ReturnResults(resultTestProbe.ref()));

        ADBJoinAttributeIntersector.Results result =
                resultTestProbe.expectMessageClass(ADBJoinAttributeIntersector.Results.class);

        assertThat(result.getCandidates().size()).isEqualTo(2);
        assertThat(result.getCandidates().get(0)).isEqualTo(setA.get(0));
        assertThat(result.getCandidates().get(1)).isEqualTo(setA.get(5));
    }

}