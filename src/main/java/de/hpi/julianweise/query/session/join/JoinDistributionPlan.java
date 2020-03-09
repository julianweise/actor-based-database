package de.hpi.julianweise.query.session.join;

import akka.actor.typed.ActorRef;
import de.hpi.julianweise.shard.ADBShard;

import java.util.AbstractMap;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class JoinDistributionPlan {

    private final List<ActorRef<ADBShard.Command>> shards;
    private final BitSet[] distributionMap;
    private final Map<Integer, AtomicInteger> dataAccesses = new HashMap<>();

    public JoinDistributionPlan(List<ActorRef<ADBShard.Command>> shards) {
        this.shards = shards;
        this.distributionMap = this.initializeDistributionMap(shards.size());
        IntStream.range(0, shards.size()).forEach(index -> this.dataAccesses.put(index, new AtomicInteger()));
    }

    private BitSet[] initializeDistributionMap(int numberOfShards) {
        BitSet[] map = new BitSet[numberOfShards];
        for (int i = 0; i < numberOfShards; i++) {
            map[i] = new BitSet(numberOfShards);
            for (int j = 0; j <= i; j++) {
                map[i].set(j);
            }
        }
        return map;
    }

    public ActorRef<ADBShard.Command> getNextJoinShardFor(ActorRef<ADBShard.Command> shard) {
        int shardIndex = this.getIndexOfShard(shard);
        int shardIndexMinimalAccesses = this.getShardIndexWithMinimalAccessesForShard(shardIndex);
        if (shardIndexMinimalAccesses < 0) {
            return null;
        }
        this.distributionMap[shardIndex].set(shardIndexMinimalAccesses);
        this.distributionMap[shardIndexMinimalAccesses].set(shardIndex);
        this.dataAccesses.get(shardIndex).incrementAndGet();
        this.dataAccesses.get(shardIndexMinimalAccesses).incrementAndGet();
        return this.shards.get(shardIndexMinimalAccesses);
    }

    private int getIndexOfShard(ActorRef<ADBShard.Command> shard) {
        return this.shards.indexOf(shard);
    }

    private int getShardIndexWithMinimalAccessesForShard(int index) {
        return this.dataAccesses.entrySet()
                                .stream()
                                .filter(eS -> !this.alreadyCompared(eS.getKey(), index))
                                .min(Comparator.comparingInt(eS -> eS.getValue().get()))
                                .orElseGet(() -> new AbstractMap.SimpleEntry<>(-1, new AtomicInteger(-1)))
                                .getKey();
    }

    private boolean alreadyCompared(int indexA, int indexB) {
        return this.distributionMap[indexA].get(indexB) && this.distributionMap[indexB].get(indexA);
    }
}
