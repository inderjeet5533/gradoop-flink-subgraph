package org.gradoop.flink.dataset;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.JoinedStreams;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface DataSet<T> {

    SingleOutputStreamOperator<T> filter(FilterFunction<T> filter);

    <T2> JoinedStreams<T, T2> join(DataStream<T2> otherStream);

    DataStream<T> union(DataStream<T>... streams);

    <K> KeyedStream<T, K> distinct(KeySelector<T, K> keyExtractor);

    <R> SingleOutputStreamOperator<R> flatMap(FlatMapFunction<T, R> flatMapper);

}
