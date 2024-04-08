package org.gradoop.flink.dataset;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Keys;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public abstract class DataSet<T> {//implements DataSet<T> {

    //    private final StreamExecutionEnvironment env;
    private final DataStream<T> internalStream;
//    private final TypeInformation<T> typeInfo;
//
//    public DataSet(StreamExecutionEnvironment env, DataStream<T> internalStream, TypeInformation<T> typeInfo) {
//        this.env = env;
//        this.internalStream = internalStream;
//        this.typeInfo = typeInfo;
//    }

    public DataSet(DataStream<T> internalStream) {
        this.internalStream = internalStream;
    }

//    public StreamExecutionEnvironment getEnv() {
//        return env;
//    }

    public DataStream<T> getInternalStream() {
        return internalStream;
    }

//    public TypeInformation<T> getTypeInfo() {
//        return typeInfo;
//    }

//    @Override
    public SingleOutputStreamOperator<T> filter(FilterFunction<T> filter) {
        if (filter == null) {
            throw new NullPointerException("Filter function must not be null.");
        } else {
            return internalStream.filter(filter);
        }
    }

//    @Override
    public <T2> JoinedStreams<T, T2> join(DataStream<T2> otherStream) {
        return internalStream.join(otherStream);
    }

//    @Override
    public DataStream<T> union(DataStream<T>... streams) {
        return internalStream.union(streams);
    }

//    @Override
    public <K> KeyedStream<T, K> distinct(KeySelector<T, K> key) {
        return internalStream.keyBy(key);
    }

    public KeyedStream<T, Tuple> distinct(int... fields) {
        return internalStream.keyBy(fields);
    }

    public KeyedStream<T, Tuple> distinct(String... fields) {
        return internalStream.keyBy(fields);
    }


//    @Override
    public <R> SingleOutputStreamOperator<R> map(MapFunction<T, R> mapper) {
        return internalStream.map(mapper);
    }

//    @Override
    public <R> SingleOutputStreamOperator<R> flatMap(FlatMapFunction<T, R> flatMapper) {
        return internalStream.flatMap(flatMapper);
    }

//    @Override
    public <K> KeyedStream<T, K> groupBy(KeySelector<T, K> key) {
        return internalStream.keyBy(key);
    }

//    @Override
    public KeyedStream<T, Tuple> groupBy(int... fields) {
        return internalStream.keyBy(fields);
    }

//    @Override
    public KeyedStream<T, Tuple> groupBy(String... fields) {
        return internalStream.keyBy(fields);
    }

//    @Override
    public <T2> CoGroupedStreams<T, T2> leftOuterJoin(DataSet<T2> other) {
        return internalStream.coGroup(other.getInternalStream());
    }

}
