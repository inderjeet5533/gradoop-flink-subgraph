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
package org.gradoop.flink.model.impl.layouts.gve;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphElement;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
//import org.gradoop.flink.model.impl.functions.epgm.*;
//import org.gradoop.flink.model.impl.functions.utils.Cast;
//import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

import java.util.Set;

/**
 * Represents a graph or a graph collection using three separate datasets:
 * - the first dataset contains the graph heads which are the meta data of logical graphs
 * - the second dataset contains the vertices contained in all graphs of the collection
 * - the third dataset contains the edges contained in all graphs of the collection
 */
public class GVELayout implements LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge>,
  GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> {
  /**
   * Graph data associated with the logical graphs in that collection.
   */
  private final DataSet<EPGMGraphHead> graphHeads;
  /**
   * DataSet containing vertices associated with that graph.
   */
  private final DataSet<EPGMVertex> vertices;
  /**
   * DataSet containing edges associated with that graph.
   */
  private final DataSet<EPGMEdge> edges;

  /**
   * Constructor
   *
   * @param graphHeads graph head dataset
   * @param vertices vertex dataset
   * @param edges edge dataset
   */
  protected GVELayout(DataSet<EPGMGraphHead> graphHeads, DataSet<EPGMVertex> vertices,
                      DataSet<EPGMEdge> edges) {
    this.graphHeads = graphHeads;
    this.vertices = vertices;
    this.edges = edges;
  }

  @Override
  public boolean isGVELayout() {
    return true;
  }

  @Override
  public boolean isIndexedGVELayout() {
    return false;
  }

  @Override
  public boolean isTransactionalLayout() {
    return false;
  }

  @Override
  public DataSet<EPGMGraphHead> getGraphHeads() {
    return graphHeads;
  }

  @Override
  public DataSet<EPGMGraphHead> getGraphHeadsByLabel(String label) {
//    return graphHeads.filter(new ByLabel<>(label));
    return null;
  }

//  @Override
//  public DataSet<GraphTransaction> getGraphTransactions() {
//    DataSet<Tuple2<GradoopId, EPGMGraphElement>> graphVertexTuples = getVertices()
//      .map(new Cast<>(EPGMGraphElement.class))
//      .returns(TypeExtractor.getForClass(EPGMGraphElement.class))
//      .flatMap(new GraphElementExpander<>());
//
//    DataSet<Tuple2<GradoopId, EPGMGraphElement>> graphEdgeTuples = getEdges()
//      .map(new Cast<>(EPGMGraphElement.class))
//      .returns(TypeExtractor.getForClass(EPGMGraphElement.class))
//      .flatMap(new GraphElementExpander<>());
//
//    DataSet<Tuple3<GradoopId, Set<EPGMVertex>, Set<EPGMEdge>>> transactions = graphVertexTuples
//      .union(graphEdgeTuples)
//      .groupBy(0)
//      .combineGroup(new GraphVerticesEdges())
//      .groupBy(0)
//      .reduceGroup(new GraphVerticesEdges());
//
//    return getGraphHeads()
//      .leftOuterJoin(transactions)
//      .where(new Id<>()).equalTo(0)
//      .with(new TransactionFromSets());
//  }

  @Override
  public DataSet<EPGMGraphHead> getGraphHead() {
    return graphHeads;
  }

  @Override
  public DataSet<EPGMVertex> getVertices() {
    return vertices;
  }

  @Override
  public DataSet<EPGMVertex> getVerticesByLabel(String label) {
//    return vertices.filter(new ByLabel<>(label));
    return null;
  }

  @Override
  public DataSet<EPGMEdge> getEdges() {
    return edges;
  }

  @Override
  public DataSet<EPGMEdge> getEdgesByLabel(String label) {
//    return edges.filter(new ByLabel<>(label));
    return null;
  }
}
