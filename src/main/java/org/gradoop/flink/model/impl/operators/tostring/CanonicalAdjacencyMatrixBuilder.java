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
package org.gradoop.flink.model.impl.operators.tostring;

//import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.dataset.DataSet;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphCollectionToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.LabelCombiner;
import org.gradoop.flink.model.impl.operators.tostring.api.EdgeToString;
import org.gradoop.flink.model.impl.operators.tostring.api.GraphHeadToString;
import org.gradoop.flink.model.impl.operators.tostring.api.VertexToString;
import org.gradoop.flink.model.impl.operators.tostring.functions.*;
import org.gradoop.flink.model.impl.operators.tostring.tuples.EdgeString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.GraphHeadString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.VertexString;

/**
 * Operator deriving a string representation from a graph collection.
 * The representation follows the concept of a canonical adjacency matrix.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> type of the base graph instance
 * @param <GC> type of the graph collection
 */
public class CanonicalAdjacencyMatrixBuilder<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>>
  implements UnaryBaseGraphCollectionToValueOperator<GC, DataSet<String>> {

  /**
   * Character used to separate lines. Should be the same regardless of OS.
   */
  public static final Character LINE_SEPARATOR = '\n';

  /**
   * function describing string representation of graph heads
   */
  private final GraphHeadToString<G> graphHeadToString;
  /**
   * function describing string representation of vertices
   */
  private final VertexToString<V> vertexToString;
  /**
   * function describing string representation of edges
   */
  private final EdgeToString<E> egeLabelingFunction;
  /**
   * sets mode for either directed or undirected graph
   */
  private final boolean directed;

  /**
   * Creates a new MatrixBuilder instance.
   *
   * @param graphHeadToString representation of graph heads
   * @param vertexToString representation of vertices
   * @param edgeLabelingFunction representation of edges
   * @param directed sets mode for either directed or undirected graph
   */
  public CanonicalAdjacencyMatrixBuilder(
    GraphHeadToString<G> graphHeadToString,
    VertexToString<V> vertexToString,
    EdgeToString<E> edgeLabelingFunction,
    boolean directed
  ) {
    this.graphHeadToString = graphHeadToString;
    this.vertexToString = vertexToString;
    this.egeLabelingFunction = edgeLabelingFunction;
    this.directed = directed;
  }

  @Override
  public DataSet<String> execute(GC collection) {

    // 1-10.
    DataSet<GraphHeadString> graphHeadLabels = getGraphHeadStrings(collection);

    // 11. add empty head to prevent empty result for empty collection

    graphHeadLabels = graphHeadLabels
      .union(collection
        .getConfig()
        .getExecutionEnvironment()
        .fromElements(new GraphHeadString(GradoopId.get(), "")));

    // 12. label collection

    return graphHeadLabels
      .reduceGroup(new ConcatGraphHeadStrings());
  }

  /**
   * Created a dataset of (graph id, canonical label) pairs.
   *
   * @param collection input collection
   * @return (graph id, canonical label) pairs
   */
  public DataSet<GraphHeadString> getGraphHeadStrings(GC collection) {
    // 1. label graph heads
    DataSet<GraphHeadString> graphHeadLabels = collection.getGraphHeads()
      .map(graphHeadToString);

    // 2. label vertices
    DataSet<VertexString> vertexLabels = collection.getVertices()
      .flatMap(vertexToString);

    // 3. label edges
    DataSet<EdgeString> edgeLabels = collection.getEdges()
      .flatMap(egeLabelingFunction);

    if (directed) {
      // 4. combine labels of parallel edges
      edgeLabels = edgeLabels
        .groupBy(0, 1, 2)
        .reduceGroup(new MultiEdgeStringCombiner());

      // 5. extend edge labels by vertex labels

      edgeLabels = edgeLabels
        .join(vertexLabels)
        .where(0, 1).equalTo(0, 1) // graphId,sourceId = graphId,vertexId
        .with(new SourceStringUpdater())
        .join(vertexLabels)
        .where(0, 2).equalTo(0, 1) // graphId,targetId = graphId,vertexId
        .with(new TargetStringUpdater());

      // 6. extend vertex labels by outgoing vertex+edge labels

      DataSet<VertexString> outgoingAdjacencyListLabels =
        edgeLabels.groupBy(0, 1) // graphId, sourceId
          .reduceGroup(new OutgoingAdjacencyList());

      // 7. extend vertex labels by outgoing vertex+edge labels

      DataSet<VertexString> incomingAdjacencyListLabels =
        edgeLabels.groupBy(0, 2) // graphId, targetId
          .reduceGroup(new IncomingAdjacencyList());

      // 8. combine vertex labels

      vertexLabels = vertexLabels
        .leftOuterJoin(outgoingAdjacencyListLabels)
        .where(0, 1).equalTo(0, 1)
        .with(new LabelCombiner<VertexString>())
        .leftOuterJoin(incomingAdjacencyListLabels)
        .where(0, 1).equalTo(0, 1)
        .with(new LabelCombiner<VertexString>());
    } else {
    // undirected graph

      // 4. union edges with flipped edges and combine labels of parallel edges

      edgeLabels = edgeLabels
        .union(edgeLabels
          .map(new SwitchSourceTargetIds()))
        .groupBy(0, 1, 2)
        .reduceGroup(new MultiEdgeStringCombiner());

      // 5. extend edge labels by vertex labels

      edgeLabels = edgeLabels
        .join(vertexLabels)
        .where(0, 2).equalTo(0, 1) // graphId,targetId = graphId,vertexId
        .with(new TargetStringUpdater());

      // 6/7. extend vertex labels by vertex+edge labels

      DataSet<VertexString> adjacencyListLabels =
        edgeLabels.groupBy(0, 1) // graphId, sourceId
          .reduceGroup(new UndirectedAdjacencyList());

      // 8. combine vertex labels

      vertexLabels = vertexLabels
        .leftOuterJoin(adjacencyListLabels)
        .where(0, 1).equalTo(0, 1)
        .with(new LabelCombiner<VertexString>());
    }

    // 9. create adjacency matrix labels

    DataSet<GraphHeadString> adjacencyMatrixLabels = vertexLabels
      .groupBy(0)
      .reduceGroup(new AdjacencyMatrix());

    // 10. combine graph labels

    graphHeadLabels = graphHeadLabels
      .leftOuterJoin(adjacencyMatrixLabels)
      .where(0).equalTo(0)
      .with(new LabelCombiner<GraphHeadString>());
    return graphHeadLabels;
  }
}
