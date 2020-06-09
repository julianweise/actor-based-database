package de.hpi.julianweise.query.session.join;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.hpi.julianweise.slave.partition.ADBPartitionManager;
import de.hpi.julianweise.slave.query.ADBQueryManager;
import de.hpi.julianweise.utility.query.join.JoinDistributionPlan;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class JoinDistributionPlanTest {

    @ClassRule
    public static TestKitJunitResource testKit = new TestKitJunitResource();

    @After
    public void cleanup() {
        testKit.after();
        testKit = new TestKitJunitResource();
        ADBPartitionManager.resetSingleton();
        ADBQueryManager.resetSingleton();
        ADBQueryManager.resetPool();
    }

    @AfterClass
    public static void after() {
        testKit.after();
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsPermutationABC() {
        TestProbe<ADBQueryManager.Command> shardA = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shardB = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shardC = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> shards = new ObjectArrayList<>();
        shards.add(shardA.ref());
        shards.add(shardB.ref());
        shards.add(shardC.ref());

        JoinDistributionPlan plan = new JoinDistributionPlan(shards);

        assertThat(plan.getNextJoinNodeFor(shardA.ref())).isEqualTo(shardB.ref());
        assertThat(plan.getNextJoinNodeFor(shardB.ref())).isEqualTo(shardC.ref());
        assertThat(plan.getNextJoinNodeFor(shardC.ref())).isEqualTo(shardA.ref());
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsPermutationACB() {
        TestProbe<ADBQueryManager.Command> shardA = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shardB = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shardC = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> shards = new ObjectArrayList<>();
        shards.add(shardA.ref());
        shards.add(shardB.ref());
        shards.add(shardC.ref());

        JoinDistributionPlan plan = new JoinDistributionPlan(shards);

        assertThat(plan.getNextJoinNodeFor(shardA.ref())).isEqualTo(shardB.ref());
        assertThat(plan.getNextJoinNodeFor(shardC.ref())).isEqualTo(shardA.ref());
        assertThat(plan.getNextJoinNodeFor(shardB.ref())).isEqualTo(shardC.ref());
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsPermutationBCA() {
        TestProbe<ADBQueryManager.Command> shardA = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shardB = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shardC = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> shards = new ObjectArrayList<>();
        shards.add(shardA.ref());
        shards.add(shardB.ref());
        shards.add(shardC.ref());

        JoinDistributionPlan plan = new JoinDistributionPlan(shards);

        assertThat(plan.getNextJoinNodeFor(shardB.ref())).isEqualTo(shardA.ref());
        assertThat(plan.getNextJoinNodeFor(shardC.ref())).isEqualTo(shardB.ref());
        assertThat(plan.getNextJoinNodeFor(shardA.ref())).isEqualTo(shardC.ref());
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsPermutationBAC() {
        TestProbe<ADBQueryManager.Command> shardA = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shardB = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shardC = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> shards = new ObjectArrayList<>();
        shards.add(shardA.ref());
        shards.add(shardB.ref());
        shards.add(shardC.ref());

        JoinDistributionPlan plan = new JoinDistributionPlan(shards);

        assertThat(plan.getNextJoinNodeFor(shardB.ref())).isEqualTo(shardA.ref());
        assertThat(plan.getNextJoinNodeFor(shardA.ref())).isEqualTo(shardC.ref());
        assertThat(plan.getNextJoinNodeFor(shardC.ref())).isEqualTo(shardB.ref());
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsPermutationCAB() {
        TestProbe<ADBQueryManager.Command> shardA = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shardB = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shardC = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> shards = new ObjectArrayList<>();
        shards.add(shardA.ref());
        shards.add(shardB.ref());
        shards.add(shardC.ref());

        JoinDistributionPlan plan = new JoinDistributionPlan(shards);

        assertThat(plan.getNextJoinNodeFor(shardC.ref())).isEqualTo(shardA.ref());
        assertThat(plan.getNextJoinNodeFor(shardA.ref())).isEqualTo(shardB.ref());
        assertThat(plan.getNextJoinNodeFor(shardB.ref())).isEqualTo(shardC.ref());
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsPermutationCBA() {
        TestProbe<ADBQueryManager.Command> shardA = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shardB = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shardC = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> shards = new ObjectArrayList<>();
        shards.add(shardA.ref());
        shards.add(shardB.ref());
        shards.add(shardC.ref());

        JoinDistributionPlan plan = new JoinDistributionPlan(shards);

        assertThat(plan.getNextJoinNodeFor(shardC.ref())).isEqualTo(shardA.ref());
        assertThat(plan.getNextJoinNodeFor(shardB.ref())).isEqualTo(shardC.ref());
        assertThat(plan.getNextJoinNodeFor(shardA.ref())).isEqualTo(shardB.ref());
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsLargerVolume() {
        TestProbe<ADBQueryManager.Command> shardA = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shardB = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shardC = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shardD = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> shards = new ObjectArrayList<>();
        shards.add(shardA.ref());
        shards.add(shardB.ref());
        shards.add(shardC.ref());
        shards.add(shardD.ref());

        JoinDistributionPlan plan = new JoinDistributionPlan(shards);

        assertThat(plan.getNextJoinNodeFor(shardA.ref())).isEqualTo(shardB.ref()); // A:1 B:1 C:0 D:0
        assertThat(plan.getNextJoinNodeFor(shardB.ref())).isEqualTo(shardC.ref()); // A:1 B:2 C:1 D:0
        assertThat(plan.getNextJoinNodeFor(shardC.ref())).isEqualTo(shardD.ref()); // A:1 B:2 C:2 D:1
        assertThat(plan.getNextJoinNodeFor(shardD.ref())).isEqualTo(shardA.ref()); // A:2 B:2 C:2 D:2
        assertThat(plan.getNextJoinNodeFor(shardA.ref())).isEqualTo(shardC.ref()); // A:3 B:2 C:3 D:2
        assertThat(plan.getNextJoinNodeFor(shardB.ref())).isEqualTo(shardD.ref()); // A:3 B:3 C:3 D:3
    }

    @Test
    public void distributionPlanIsFulfilledAfterAllPermutationsHaveBeenTested() {
        TestProbe<ADBQueryManager.Command> shardA = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shardB = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shardC = testKit.createTestProbe();
        TestProbe<ADBQueryManager.Command> shardD = testKit.createTestProbe();

        ObjectList<ActorRef<ADBQueryManager.Command>> shards = new ObjectArrayList<>();
        shards.add(shardA.ref());
        shards.add(shardB.ref());
        shards.add(shardC.ref());
        shards.add(shardD.ref());

        JoinDistributionPlan plan = new JoinDistributionPlan(shards);

        assertThat(plan.getNextJoinNodeFor(shardA.ref())).isEqualTo(shardB.ref());
        assertThat(plan.getNextJoinNodeFor(shardB.ref())).isEqualTo(shardC.ref());
        assertThat(plan.getNextJoinNodeFor(shardB.ref())).isEqualTo(shardD.ref());
        assertThat(plan.getNextJoinNodeFor(shardD.ref())).isEqualTo(shardA.ref());
        assertThat(plan.getNextJoinNodeFor(shardC.ref())).isEqualTo(shardA.ref());
        assertThat(plan.getNextJoinNodeFor(shardD.ref())).isEqualTo(shardC.ref());
    }
}