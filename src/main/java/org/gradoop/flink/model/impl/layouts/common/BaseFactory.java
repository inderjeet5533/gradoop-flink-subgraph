/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.layouts.common;

import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.api.entities.GraphHeadFactory;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.*;
import org.gradoop.flink.model.api.layouts.BaseLayoutFactory;
import org.gradoop.flink.model.impl.functions.bool.False;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Collection;
import java.util.Objects;

/**
 * Base class for graph layout factories.
 */
public abstract class BaseFactory implements BaseLayoutFactory<EPGMGraphHead, EPGMVertex, EPGMEdge> {

  /**
   * Gradoop Flink config
   */
  private GradoopFlinkConfig config;

  /**
   * Knows how to create {@link EPGMGraphHead}
   */
  private final EPGMGraphHeadFactory graphHeadFactory;

  /**
   * Knows how to create {@link EPGMVertex}
   */
  private final EPGMVertexFactory vertexFactory;

  /**
   *  Knows how to create {@link EPGMEdge}
   */
  private final EPGMEdgeFactory edgeFactory;

  /**
   * Creates a new Configuration.
   */
  protected BaseFactory() {
    this.graphHeadFactory = new EPGMGraphHeadFactory();
    this.vertexFactory = new EPGMVertexFactory();
    this.edgeFactory = new EPGMEdgeFactory();
  }

  @Override
  public GraphHeadFactory<EPGMGraphHead> getGraphHeadFactory() {
    return graphHeadFactory;
  }

  @Override
  public VertexFactory<EPGMVertex> getVertexFactory() {
    return vertexFactory;
  }

  @Override
  public EdgeFactory<EPGMEdge> getEdgeFactory() {
    return edgeFactory;
  }

  @Override
  public void setGradoopFlinkConfig(GradoopFlinkConfig config) {
    Objects.requireNonNull(config);
    this.config = config;
  }

  protected GradoopFlinkConfig getConfig() {
    return config;
  }

  /**
   * Creates a graph head dataset from a given collection.
   * Encapsulates the workaround for dataset creation from an empty collection.
   *
   * @param graphHeads graph heads
   * @return graph head dataset
   */
  protected DataStream<EPGMGraphHead> createGraphHeadDataSet(Collection<EPGMGraphHead> graphHeads) {

    StreamExecutionEnvironment env = getConfig().getExecutionEnvironment();

    DataStream<EPGMGraphHead> graphHeadSet;
    if (graphHeads.isEmpty()) {
      graphHeadSet = env
        .fromElements(getGraphHeadFactory().createGraphHead())
        .filter(new False<>());
    } else {
      graphHeadSet =  env.fromCollection(graphHeads);
    }
    return graphHeadSet;
  }

  /**
   * Creates a vertex dataset from a given collection.
   * Encapsulates the workaround for dataset creation from an empty collection.
   *
   * @param vertices  vertex collection
   * @return vertex dataset
   */
  protected DataStream<EPGMVertex> createVertexDataSet(Collection<EPGMVertex> vertices) {

    StreamExecutionEnvironment env = getConfig().getExecutionEnvironment();

    DataStream<EPGMVertex> vertexSet;
    if (vertices.isEmpty()) {
      vertexSet = env
        .fromElements(getVertexFactory().createVertex())
        .filter(new False<>());
    } else {
      vertexSet = env.fromCollection(vertices);
    }
    return vertexSet;
  }

  /**
   * Creates an edge dataset from a given collection.
   * Encapsulates the workaround for dataset creation from an empty collection.
   *
   * @param edges edge collection
   * @return edge dataset
   */
  protected DataStream<EPGMEdge> createEdgeDataSet(Collection<EPGMEdge> edges) {
    StreamExecutionEnvironment env = getConfig().getExecutionEnvironment();

    DataStream<EPGMEdge> edgeSet;
    if (edges.isEmpty()) {
      GradoopId dummyId = GradoopId.get();
      edgeSet = env
        .fromElements(getEdgeFactory().createEdge(dummyId, dummyId))
        .filter(new False<>());
    } else {
      edgeSet = env.fromCollection(edges);
    }
    return edgeSet;
  }
}
