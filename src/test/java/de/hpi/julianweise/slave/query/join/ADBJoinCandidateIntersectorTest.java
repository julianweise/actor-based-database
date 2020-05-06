package de.hpi.julianweise.slave.query.join;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.slave.query.join.column.intersect.ADBJoinCandidateIntersector;
import de.hpi.julianweise.slave.query.join.column.intersect.ADBJoinCandidateIntersectorFactory;
import de.hpi.julianweise.utility.largemessage.ADBKeyPair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ADBJoinCandidateIntersectorTest {

    @ClassRule
    public static TestKitJunitResource testKit = new TestKitJunitResource();


    @After
    public void cleanup() {
        testKit.after();
        testKit = new TestKitJunitResource();
        ADBPartitionManager.resetSingleton();
        ADBQueryManager.resetPool();
        ADBQueryManager.resetSingleton();
    }

    @AfterClass
    public static void after() {
        testKit.after();
        ADBPartitionManager.resetSingleton();
        ADBQueryManager.resetPool();
        ADBQueryManager.resetSingleton();
    }

    @Test
    public void intersectorJustFilled() {
        TestProbe<ADBJoinCandidateIntersector.Results> resultTestProbe = testKit.createTestProbe();
        ADBJoinQuery query = new ADBJoinQuery();
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "aInteger", "aInteger"));
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "bString", "bString"));

        List<ADBKeyPair> setA = new ArrayList<>();
        setA.add(new ADBKeyPair(1,2));
        setA.add(new ADBKeyPair(2,3));
        setA.add(new ADBKeyPair(3,4));
        setA.add(new ADBKeyPair(4,5));
        setA.add(new ADBKeyPair(5,6));

        ActorRef<ADBJoinCandidateIntersector.Command> intersector = testKit.spawn(
                ADBJoinCandidateIntersectorFactory.createDefault());

        intersector.tell(new ADBJoinCandidateIntersector.Intersect(setA));

        intersector.tell(new ADBJoinCandidateIntersector.ReturnResults(resultTestProbe.ref()));

        ADBJoinCandidateIntersector.Results result =
                resultTestProbe.expectMessageClass(ADBJoinCandidateIntersector.Results.class);


        assertThat(result.getCandidates().size()).isEqualTo(setA.size());
    }

    @Test
    public void intersectEmptyLists() {
        TestProbe<ADBJoinCandidateIntersector.Results> resultTestProbe = testKit.createTestProbe();
        ADBJoinQuery query = new ADBJoinQuery();
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "aInteger", "aInteger"));
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "bString", "bString"));

        List<ADBKeyPair> setA = new ArrayList<>();
        List<ADBKeyPair> setB = new ArrayList<>();

        ActorRef<ADBJoinCandidateIntersector.Command> intersector = testKit.spawn(
                ADBJoinCandidateIntersectorFactory.createDefault());

        intersector.tell(new ADBJoinCandidateIntersector.Intersect(setA));
        intersector.tell(new ADBJoinCandidateIntersector.Intersect(setB));

        intersector.tell(new ADBJoinCandidateIntersector.ReturnResults(resultTestProbe.ref()));

        ADBJoinCandidateIntersector.Results result =
                resultTestProbe.expectMessageClass(ADBJoinCandidateIntersector.Results.class);


        assertThat(result.getCandidates().size()).isZero();
    }

    @Test
    public void intersectWithOneEmptyList() {
        TestProbe<ADBJoinCandidateIntersector.Results> resultTestProbe = testKit.createTestProbe();
        ADBJoinQuery query = new ADBJoinQuery();
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "aInteger", "aInteger"));
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "bString", "bString"));

        List<ADBKeyPair> setA = new ArrayList<>(5);
        setA.add(new ADBKeyPair(1,2));
        setA.add(new ADBKeyPair(2,3));
        setA.add(new ADBKeyPair(3,4));
        setA.add(new ADBKeyPair(4,5));
        setA.add(new ADBKeyPair(5,6));
        List<ADBKeyPair> setB = new ArrayList<>();

        ActorRef<ADBJoinCandidateIntersector.Command> intersector = testKit.spawn(
                ADBJoinCandidateIntersectorFactory.createDefault());

        intersector.tell(new ADBJoinCandidateIntersector.Intersect(setA));
        intersector.tell(new ADBJoinCandidateIntersector.Intersect(setB));

        intersector.tell(new ADBJoinCandidateIntersector.ReturnResults(resultTestProbe.ref()));

        ADBJoinCandidateIntersector.Results result =
                resultTestProbe.expectMessageClass(ADBJoinCandidateIntersector.Results.class);

        assertThat(result.getCandidates().size()).isZero();
    }

    @Test
    public void intersectWithOtherEmptyList() {
        TestProbe<ADBJoinCandidateIntersector.Results> resultTestProbe = testKit.createTestProbe();
        ADBJoinQuery query = new ADBJoinQuery();
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "aInteger", "aInteger"));
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "bString", "bString"));

        List<ADBKeyPair> setA = new ArrayList<>(5);
        setA.add(new ADBKeyPair(1,2));
        setA.add(new ADBKeyPair(2,3));
        setA.add(new ADBKeyPair(3,4));
        setA.add(new ADBKeyPair(4,5));
        setA.add(new ADBKeyPair(5,6));
        List<ADBKeyPair> setB = new ArrayList<>();

        ActorRef<ADBJoinCandidateIntersector.Command> intersector = testKit.spawn(
                ADBJoinCandidateIntersectorFactory.createDefault());

        intersector.tell(new ADBJoinCandidateIntersector.Intersect(setB));
        intersector.tell(new ADBJoinCandidateIntersector.Intersect(setA));

        intersector.tell(new ADBJoinCandidateIntersector.ReturnResults(resultTestProbe.ref()));

        ADBJoinCandidateIntersector.Results result =
                resultTestProbe.expectMessageClass(ADBJoinCandidateIntersector.Results.class);

        assertThat(result.getCandidates().size()).isZero();
    }

    @Test
    public void intersectTwoFilledListsCorrectly() {
        TestProbe<ADBJoinCandidateIntersector.Results> resultTestProbe = testKit.createTestProbe();
        ADBJoinQuery query = new ADBJoinQuery();
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "aInteger", "aInteger"));
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "bString", "bString"));

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


        ActorRef<ADBJoinCandidateIntersector.Command> intersector = testKit.spawn(
                ADBJoinCandidateIntersectorFactory.createDefault());

        intersector.tell(new ADBJoinCandidateIntersector.Intersect(setA));
        intersector.tell(new ADBJoinCandidateIntersector.Intersect(setB));

        intersector.tell(new ADBJoinCandidateIntersector.ReturnResults(resultTestProbe.ref()));

        ADBJoinCandidateIntersector.Results result =
                resultTestProbe.expectMessageClass(ADBJoinCandidateIntersector.Results.class);

        assertThat(result.getCandidates().size()).isEqualTo(2);
        assertThat(result.getCandidates().get(0)).isEqualTo(setA.get(0));
        assertThat(result.getCandidates().get(1)).isEqualTo(setA.get(3));
    }

    @Test
    public void intersectTwoFilledListsCorrectlyRemoveNotIntersectedDuplicates() {
        TestProbe<ADBJoinCandidateIntersector.Results> resultTestProbe = testKit.createTestProbe();
        ADBJoinQuery query = new ADBJoinQuery();
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "aInteger", "aInteger"));
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "bString", "bString"));

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

        ActorRef<ADBJoinCandidateIntersector.Command> intersector = testKit.spawn(
                ADBJoinCandidateIntersectorFactory.createDefault());

        intersector.tell(new ADBJoinCandidateIntersector.Intersect(setA));
        intersector.tell(new ADBJoinCandidateIntersector.Intersect(setB));

        intersector.tell(new ADBJoinCandidateIntersector.ReturnResults(resultTestProbe.ref()));

        ADBJoinCandidateIntersector.Results result =
                resultTestProbe.expectMessageClass(ADBJoinCandidateIntersector.Results.class);

        assertThat(result.getCandidates().size()).isEqualTo(2);
        assertThat(result.getCandidates().get(0)).isEqualTo(setA.get(0));
        assertThat(result.getCandidates().get(1)).isEqualTo(setA.get(4));
    }

    @Test
    public void intersectTwoFilledListsCorrectlyRemoveIntersectedDuplicates() {
        TestProbe<ADBJoinCandidateIntersector.Results> resultTestProbe = testKit.createTestProbe();
        ADBJoinQuery query = new ADBJoinQuery();
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "aInteger", "aInteger"));
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "bString", "bString"));

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

        ActorRef<ADBJoinCandidateIntersector.Command> intersector = testKit.spawn(
                ADBJoinCandidateIntersectorFactory.createDefault());

        intersector.tell(new ADBJoinCandidateIntersector.Intersect(setA));
        intersector.tell(new ADBJoinCandidateIntersector.Intersect(setB));

        intersector.tell(new ADBJoinCandidateIntersector.ReturnResults(resultTestProbe.ref()));

        ADBJoinCandidateIntersector.Results result =
                resultTestProbe.expectMessageClass(ADBJoinCandidateIntersector.Results.class);

        assertThat(result.getCandidates().size()).isEqualTo(2);
        assertThat(result.getCandidates().get(0)).isEqualTo(setA.get(0));
        assertThat(result.getCandidates().get(1)).isEqualTo(setA.get(5));
    }

    @Test
    public void intersectTwoUnequallyFilledListsCorrectly() {
        TestProbe<ADBJoinCandidateIntersector.Results> resultTestProbe = testKit.createTestProbe();
        ADBJoinQuery query = new ADBJoinQuery();
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "aInteger", "aInteger"));
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "bString", "bString"));


        List<ADBKeyPair> setA = new ArrayList<>(6);
        setA.add(new ADBKeyPair(1,2));
        setA.add(new ADBKeyPair(2,3));
        setA.add(new ADBKeyPair(3,4));
        setA.add(new ADBKeyPair(6,4));
        setA.add(new ADBKeyPair(4,5));
        setA.add(new ADBKeyPair(5,6));
        List<ADBKeyPair> setB = new ArrayList<>(3);
        setB.add(new ADBKeyPair(1,2));
        setB.add(new ADBKeyPair(3,3));
        setB.add(new ADBKeyPair(4,5));

        ActorRef<ADBJoinCandidateIntersector.Command> intersector = testKit.spawn(
                ADBJoinCandidateIntersectorFactory.createDefault());

        intersector.tell(new ADBJoinCandidateIntersector.Intersect(setA));
        intersector.tell(new ADBJoinCandidateIntersector.Intersect(setB));

        intersector.tell(new ADBJoinCandidateIntersector.ReturnResults(resultTestProbe.ref()));

        ADBJoinCandidateIntersector.Results result =
                resultTestProbe.expectMessageClass(ADBJoinCandidateIntersector.Results.class);

        assertThat(result.getCandidates().size()).isEqualTo(2);
        assertThat(result.getCandidates().get(0).getKey()).isEqualTo(1);
        assertThat(result.getCandidates().get(0).getValue()).isEqualTo(2);
        assertThat(result.getCandidates().get(1).getKey()).isEqualTo(4);
        assertThat(result.getCandidates().get(1).getValue()).isEqualTo(5);
    }

    @Test
    public void intersectTwoUnequallyFilled2ListsCorrectly() {
        TestProbe<ADBJoinCandidateIntersector.Results> resultTestProbe = testKit.createTestProbe();
        ADBJoinQuery query = new ADBJoinQuery();
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "aInteger", "aInteger"));
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.LESS, "bString", "bString"));

        List<ADBKeyPair> setA = new ArrayList<>(6);
        setA.add(new ADBKeyPair(4,6));
        List<ADBKeyPair> setB = new ArrayList<>(3);
        setB.add(new ADBKeyPair(6,3));
        setB.add(new ADBKeyPair(3,1));
        setB.add(new ADBKeyPair(6,97));
        setB.add(new ADBKeyPair(12,34));
        setB.add(new ADBKeyPair(3,7));
        setB.add(new ADBKeyPair(8,2));
        setB.add(new ADBKeyPair(4,6));
        setB.add(new ADBKeyPair(0,0));
        setB.add(new ADBKeyPair(0,1));

        ActorRef<ADBJoinCandidateIntersector.Command> intersector = testKit.spawn(
                ADBJoinCandidateIntersectorFactory.createDefault());

        intersector.tell(new ADBJoinCandidateIntersector.Intersect(setA));
        intersector.tell(new ADBJoinCandidateIntersector.Intersect(setB));

        intersector.tell(new ADBJoinCandidateIntersector.ReturnResults(resultTestProbe.ref()));

        ADBJoinCandidateIntersector.Results result =
                resultTestProbe.expectMessageClass(ADBJoinCandidateIntersector.Results.class);

        assertThat(result.getCandidates().size()).isEqualTo(1);
        assertThat(result.getCandidates().get(0)).isEqualTo(setA.get(0));
    }
}