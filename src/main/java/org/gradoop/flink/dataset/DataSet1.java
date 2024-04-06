package org.gradoop.flink.dataset;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.operators.join.JoinOperatorSetsBase;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.*;

public interface DataSet1<T> {

    SingleOutputStreamOperator<T> filter(FilterFunction<T> filter);

    <T2> JoinedStreams<T, T2> join(DataStream<T2> otherStream);

    DataStream<T> union(DataStream<T>... streams);

    <K> KeyedStream<T, K> distinct(KeySelector<T, K> keyExtractor);

    <R> SingleOutputStreamOperator<R> map(MapFunction<T, R> mapper);

    <R> SingleOutputStreamOperator<R> flatMap(FlatMapFunction<T, R> flatMapper);

    <K> KeyedStream<T, K> groupBy(KeySelector<T, K> keyExtractor);

    KeyedStream<T, Tuple> groupBy(int... fields);

    KeyedStream<T, Tuple> groupBy(String... fields);

    <T2> CoGroupedStreams<T, T2> leftOuterJoin(DataSet<T2> other);

}
