package de.hpi.julianweise.query.session.join;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import de.hpi.julianweise.csv.TestEntity;
import de.hpi.julianweise.domain.ADBEntity;
import de.hpi.julianweise.query.ADBJoinQuery;
import de.hpi.julianweise.query.ADBJoinQueryTerm;
import de.hpi.julianweise.query.ADBQueryTerm;
import de.hpi.julianweise.query.ADBShardInquirer;
import de.hpi.julianweise.query.session.ADBQuerySession;
import de.hpi.julianweise.query.session.ADBQuerySessionFactory;
import de.hpi.julianweise.shard.ADBShard;
import de.hpi.julianweise.shard.query_operation.join.ADBJoinQuerySessionHandler;
import de.hpi.julianweise.utility.largemessage.ADBPair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

public class ADBJoinQuerySessionTest {

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
    public void expectCorrectRegistrationAtStartUp() {

        TestProbe<ADBShardInquirer.Command> supervisor = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shard = testKit.createTestProbe();

        ADBJoinQuery query = new ADBJoinQuery();
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ActorRef<ADBJoinQuerySession.Command> joinSession =
                testKit.spawn(ADBQuerySessionFactory.create(Collections.singletonList(shard.ref()), query, 1,
                        supervisor.ref()));

        ADBShard.QueryEntities queryEntities = shard.expectMessageClass(ADBShard.QueryEntities.class);

        assertThat(queryEntities.getQuery()).isEqualTo(query);
        assertThat(queryEntities.getTransactionId()).isEqualTo(1);
        assertThat(queryEntities.getRespondTo()).isEqualTo(joinSession);
    }

    @Test
    public void expectNoShardToJoinWithIsSentForOnlyOneShard() {

        TestProbe<ADBShardInquirer.Command> supervisor = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shard = testKit.createTestProbe();
        TestProbe<ADBJoinQuerySessionHandler.Command> joinSessionHandler = testKit.createTestProbe();

        ADBJoinQuery query = new ADBJoinQuery();
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ActorRef<ADBJoinQuerySession.Command> joinSession =
                testKit.spawn(ADBQuerySessionFactory.create(Collections.singletonList(shard.ref()), query, 1,
                        supervisor.ref()));

        joinSession.tell(new ADBQuerySession.RegisterQuerySessionHandler(shard.ref(), joinSessionHandler.ref()));


        joinSession.tell(new ADBJoinQuerySession.RequestNextShardComparison(shard.ref(), joinSessionHandler.ref()));

        ADBJoinQuerySessionHandler.NoMoreShardsToJoinWith response =
                joinSessionHandler.expectMessageClass(ADBJoinQuerySessionHandler.NoMoreShardsToJoinWith.class);

        assertThat(response.getTransactionId()).isEqualTo(1);
    }

    @Test
    public void expectCorrectNextJoinSuggestionForEachShard() {

        TestProbe<ADBShardInquirer.Command> supervisor = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shard1 = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shard2 = testKit.createTestProbe();
        TestProbe<ADBJoinQuerySessionHandler.Command> joinSessionHandler1 = testKit.createTestProbe();
        TestProbe<ADBJoinQuerySessionHandler.Command> joinSessionHandler2 = testKit.createTestProbe();

        ArrayList<ActorRef<ADBShard.Command>> shards = new ArrayList<>();
        shards.add(shard1.ref());
        shards.add(shard2.ref());

        ADBJoinQuery query = new ADBJoinQuery();
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ActorRef<ADBJoinQuerySession.Command> joinSession =
                testKit.spawn(ADBQuerySessionFactory.create(shards, query, 1, supervisor.ref()));


        joinSession.tell(new ADBQuerySession.RegisterQuerySessionHandler(shard1.ref(), joinSessionHandler1.ref()));
        joinSession.tell(new ADBQuerySession.RegisterQuerySessionHandler(shard2.ref(), joinSessionHandler2.ref()));

        joinSession.tell(new ADBJoinQuerySession.RequestNextShardComparison(shard1.ref(), joinSessionHandler1.ref()));

        ADBJoinQuerySessionHandler.JoinWithShard response1 = joinSessionHandler1
                .expectMessageClass(ADBJoinQuerySessionHandler.JoinWithShard.class);

        joinSession.tell(new ADBJoinQuerySession.RequestNextShardComparison(shard2.ref(), joinSessionHandler2.ref()));

        ADBJoinQuerySessionHandler.NoMoreShardsToJoinWith response2 = joinSessionHandler2
                .expectMessageClass(ADBJoinQuerySessionHandler.NoMoreShardsToJoinWith.class);

        assertThat(response1.getCounterpart()).isEqualTo(joinSessionHandler2.ref());
        assertThat(response2.getTransactionId()).isEqualTo(1);
    }

    @Test
    public void expectCorrectNextJoinSuggestionForEachShardEvenAfterDelayedHandlerMapping() {

        TestProbe<ADBShardInquirer.Command> supervisor = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shard1 = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shard2 = testKit.createTestProbe();
        TestProbe<ADBJoinQuerySessionHandler.Command> joinSessionHandler1 = testKit.createTestProbe();
        TestProbe<ADBJoinQuerySessionHandler.Command> joinSessionHandler2 = testKit.createTestProbe();

        ArrayList<ActorRef<ADBShard.Command>> shards = new ArrayList<>();
        shards.add(shard1.ref());
        shards.add(shard2.ref());

        ADBJoinQuery query = new ADBJoinQuery();
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ActorRef<ADBJoinQuerySession.Command> joinSession =
                testKit.spawn(ADBQuerySessionFactory.create(shards, query, 1, supervisor.ref()));

        joinSession.tell(new ADBJoinQuerySession.TriggerShardComparison(shard1.ref(), shard2.ref(),
                joinSessionHandler1.ref()));

        joinSession.tell(new ADBQuerySession.RegisterQuerySessionHandler(shard1.ref(), joinSessionHandler1.ref()));
        joinSession.tell(new ADBQuerySession.RegisterQuerySessionHandler(shard2.ref(), joinSessionHandler2.ref()));

        ADBJoinQuerySessionHandler.JoinWithShard response1 = joinSessionHandler1
                .expectMessageClass(ADBJoinQuerySessionHandler.JoinWithShard.class);

        assertThat(response1.getCounterpart()).isEqualTo(joinSessionHandler2.ref());
    }

    @Test
    public void expectJoinQueryResultsDeliveryAfterTransactionConclusion() {

        TestProbe<ADBShardInquirer.Command> supervisor = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shard1 = testKit.createTestProbe();
        TestProbe<ADBShard.Command> shard2 = testKit.createTestProbe();
        TestProbe<ADBJoinQuerySessionHandler.Command> joinSessionHandler1 = testKit.createTestProbe();
        TestProbe<ADBJoinQuerySessionHandler.Command> joinSessionHandler2 = testKit.createTestProbe();

        ArrayList<ActorRef<ADBShard.Command>> shards = new ArrayList<>();
        shards.add(shard1.ref());
        shards.add(shard2.ref());

        ADBJoinQuery query = new ADBJoinQuery();
        query.addTerm(new ADBJoinQueryTerm(ADBQueryTerm.RelationalOperator.EQUALITY, "aInteger", "aInteger"));

        ActorRef<ADBJoinQuerySession.Command> joinSession =
                testKit.spawn(ADBQuerySessionFactory.create(shards, query, 1, supervisor.ref()));


        joinSession.tell(new ADBQuerySession.RegisterQuerySessionHandler(shard1.ref(), joinSessionHandler1.ref()));
        joinSession.tell(new ADBQuerySession.RegisterQuerySessionHandler(shard2.ref(), joinSessionHandler2.ref()));

        ADBEntity joinPartnerA = new TestEntity(1, "Test", 1f, true, 1.1, 'a');
        ADBEntity joinPartnerB = new TestEntity(2, "Test", 1f, true, 1.1, 'a');
        ADBPair<ADBEntity, ADBEntity> joinResults = new ADBPair<>(joinPartnerA, joinPartnerB);

        joinSession.tell(ADBJoinQuerySession.JoinQueryResults.builder()
                                                             .transactionId(1)
                                                             .joinResults(Collections.singletonList(joinResults))
                                                             .globalShardId(1)
                                                             .build());

        // Receive self-join results first to decrease partial result counter
        joinSession.tell(new ADBJoinQuerySession.JoinQueryResults(Collections.emptyList()));

        joinSession.tell(new ADBQuerySession.ConcludeTransaction(shard1.ref(), 1));
        joinSession.tell(new ADBQuerySession.ConcludeTransaction(shard2.ref(), 1));

        ADBJoinQuerySessionHandler.Terminate response1 = joinSessionHandler1
                .expectMessageClass(ADBJoinQuerySessionHandler.Terminate.class);

        ADBJoinQuerySessionHandler.Terminate response2 = joinSessionHandler2
                .expectMessageClass(ADBJoinQuerySessionHandler.Terminate.class);

        assertThat(response1.getTransactionId()).isEqualTo(1);
        assertThat(response2.getTransactionId()).isEqualTo(1);

        ADBShardInquirer.TransactionResults response3 = supervisor
                .expectMessageClass(ADBShardInquirer.TransactionResults.class);

        assertThat(response3.getTransactionId()).isEqualTo(1);
        assertThat(response3.getResults().length).isEqualTo(1);
        assertThat(response3.getResults()[0]).isEqualTo(joinResults);
    }

}