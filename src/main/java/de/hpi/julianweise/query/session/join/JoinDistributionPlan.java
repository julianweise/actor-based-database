package de.hpi.julianweise.query.session.join;

import akka.actor.typed.ActorRef;
import de.hpi.julianweise.shard.ADBShard;
import org.slf4j.Logger;

import java.util.AbstractMap;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JoinDistributionPlan {

    private final List<ActorRef<ADBShard.Command>> shards;
    private final BitSet[] distributionMap;
    private final Map<Integer, AtomicInteger> dataAccesses = new HashMap<>();
    private final Map<Integer, AtomicInteger> dataRequests = new HashMap<>();
    private final Logger logger;

    public JoinDistributionPlan(List<ActorRef<ADBShard.Command>> shards, Logger logger) {
        this.shards = shards;
        this.logger = logger;
        this.distributionMap = this.initializeDistributionMap(shards.size());
        IntStream.range(0, shards.size()).forEach(index -> this.dataAccesses.put(index, new AtomicInteger()));
        IntStream.range(0, shards.size()).forEach(index -> this.dataRequests.put(index, new AtomicInteger()));
    }

    private BitSet[] initializeDistributionMap(int numberOfShards) {
        BitSet[] map = new BitSet[numberOfShards];
        for (int i = 0; i < numberOfShards; i++) {
            map[i] = new BitSet(numberOfShards);
            for (int j = 0; j < i; j++) {
                map[i].set(j);
            }
        }
        return map;
    }

    public ActorRef<ADBShard.Command> getNextJoinShardFor(ActorRef<ADBShard.Command> shard) {
        int shardIndex = this.getIndexOfShard(shard);
        int shardIndexMinimalAccesses = this.getShardIndexWithMinimalAccessesForShard(shardIndex);
        this.logger.info(String.format("[DistributionPlan] Shard #%d requested new shard to join. Suggested shard # %d",
                shardIndex, shardIndexMinimalAccesses));
        if (shardIndexMinimalAccesses < 0) {
            return null;
        }
        this.distributionMap[shardIndex].set(shardIndexMinimalAccesses);
        this.distributionMap[shardIndexMinimalAccesses].set(shardIndex);
        this.dataAccesses.get(shardIndex).incrementAndGet();
        this.dataAccesses.get(shardIndexMinimalAccesses).incrementAndGet();
        this.dataRequests.get(shardIndex).incrementAndGet();
        return this.shards.get(shardIndexMinimalAccesses);
    }

    private int getIndexOfShard(ActorRef<ADBShard.Command> shard) {
        return this.shards.indexOf(shard);
    }

    private int getShardIndexWithMinimalAccessesForShard(int index) {
        List<Integer> relevantAccesses = this.dataAccesses
                .entrySet().stream().filter(eS -> this.notCompared(eS.getKey(), index)).map(eS -> eS.getValue().get()).collect(Collectors.toList());
        int minDataAccesses = relevantAccesses.size() > 0 ? Collections.min(relevantAccesses) : Integer.MAX_VALUE;
        return this.dataAccesses.entrySet()
                                .stream()
                                .filter(eS -> this.notCompared(eS.getKey(), index))
                                .filter(eS1 -> eS1.getValue().get() <= minDataAccesses)
                                .min((eS1, eS2) -> this.dataRequests.get(eS2.getKey()).get() - this.dataRequests.get(eS1.getKey()).get())
                                .orElseGet(() -> new AbstractMap.SimpleEntry<>(-1, new AtomicInteger(-1)))
                                .getKey();
    }

    private boolean notCompared(int indexA, int indexB) {
        return !(this.distributionMap[indexA].get(indexB) && this.distributionMap[indexB].get(indexA));
    }
}
