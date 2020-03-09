package de.hpi.julianweise.query.session.join;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.hpi.julianweise.shard.ADBShard;
import main.de.hpi.julianweise.shard.ADBShardTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


public class JoinDistributionPlanTest {

    @ClassRule
    public static TestKitJunitResource testKit = new TestKitJunitResource(ADBShardTest.config);

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
    public void expectEvenlyIncreasingSuggestions() {
        TestProbe<ADBShard.Command> shardA = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shardB = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shardC = testKit.createTestProbe();

        List<ActorRef<ADBShard.Command>> shards = new ArrayList<>();
        shards.add(shardA.ref());
        shards.add(shardB.ref());
        shards.add(shardC.ref());

        JoinDistributionPlan plan = new JoinDistributionPlan(shards);

        assertThat(plan.getNextJoinShardFor(shardA.ref())).isEqualTo(shardB.ref());
        assertThat(plan.getNextJoinShardFor(shardB.ref())).isEqualTo(shardC.ref());
        assertThat(plan.getNextJoinShardFor(shardC.ref())).isEqualTo(shardA.ref());
    }

    @Test
    public void expectEvenlyDistributedSuggestions() {
        TestProbe<ADBShard.Command> shardA = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shardB = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shardC = testKit.createTestProbe();

        List<ActorRef<ADBShard.Command>> shards = new ArrayList<>();
        shards.add(shardA.ref());
        shards.add(shardB.ref());
        shards.add(shardC.ref());

        JoinDistributionPlan plan = new JoinDistributionPlan(shards);

        assertThat(plan.getNextJoinShardFor(shardA.ref())).isEqualTo(shardB.ref());
        assertThat(plan.getNextJoinShardFor(shardA.ref())).isEqualTo(shardC.ref());
        assertThat(plan.getNextJoinShardFor(shardC.ref())).isEqualTo(shardB.ref());
    }

    @Test
    public void expectEvenlyDistributedSuggestions2() {
        TestProbe<ADBShard.Command> shardA = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shardB = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shardC = testKit.createTestProbe();

        List<ActorRef<ADBShard.Command>> shards = new ArrayList<>();
        shards.add(shardA.ref());
        shards.add(shardB.ref());
        shards.add(shardC.ref());

        JoinDistributionPlan plan = new JoinDistributionPlan(shards);

        assertThat(plan.getNextJoinShardFor(shardA.ref())).isEqualTo(shardB.ref());
        assertThat(plan.getNextJoinShardFor(shardC.ref())).isEqualTo(shardA.ref());
        assertThat(plan.getNextJoinShardFor(shardB.ref())).isEqualTo(shardC.ref());
    }

    @Test
    public void expectEvenlyIncreasingSuggestionsLargerVolume() {
        TestProbe<ADBShard.Command> shardA = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shardB = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shardC = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shardD = testKit.createTestProbe();

        List<ActorRef<ADBShard.Command>> shards = new ArrayList<>();
        shards.add(shardA.ref());
        shards.add(shardB.ref());
        shards.add(shardC.ref());
        shards.add(shardD.ref());

        JoinDistributionPlan plan = new JoinDistributionPlan(shards);

        assertThat(plan.getNextJoinShardFor(shardA.ref())).isEqualTo(shardB.ref()); // A:1 B:1 C:0 D:0
        assertThat(plan.getNextJoinShardFor(shardB.ref())).isEqualTo(shardC.ref()); // A:1 B:2 C:1 D:0
        assertThat(plan.getNextJoinShardFor(shardB.ref())).isEqualTo(shardD.ref()); // A:1 B:3 C:1 D:1
        assertThat(plan.getNextJoinShardFor(shardD.ref())).isEqualTo(shardA.ref()); // A:2 B:3 C:1 D:2
        assertThat(plan.getNextJoinShardFor(shardC.ref())).isEqualTo(shardA.ref()); // A:3 B:3 C:2 D:2
        assertThat(plan.getNextJoinShardFor(shardD.ref())).isEqualTo(shardC.ref()); // A:3 B:3 C:3 D:3
    }

}