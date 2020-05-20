package de.hpi.julianweise.utility.list;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class IntArrayListCollector implements Collector<Integer, IntList, IntList> {
    @Override
    public Supplier<IntList> supplier() {
        return IntArrayList::new;
    }

    @Override
    public BiConsumer<IntList, Integer> accumulator() {
        return (collection, boxedValue) -> collection.add(boxedValue.intValue());
    }

    @Override
    public BinaryOperator<IntList> combiner() {
        return (c1, c2) -> {
            c1.addAll(c2);
            return c1;
        };
    }

    @Override
    public Function<IntList, IntList> finisher() {
        return x -> x;
    }

    @Override public Set<Characteristics> characteristics() {
        return new HashSet<Characteristics>() {{
            add(Characteristics.IDENTITY_FINISH);
        }};
    }
}
