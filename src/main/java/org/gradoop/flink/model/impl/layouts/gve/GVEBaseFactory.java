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
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.layouts.common.BaseFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Base class for GVE layout factories.
 */
abstract class GVEBaseFactory extends BaseFactory {

  /**
   * Creates a collection layout from the given datasets.
   *
   * @param graphHeads EPGMGraphHead DataSet
   * @param vertices EPGMVertex DataSet
   * @param edges EPGMEdge DataSet
   * @return GVE layout
   */
  GVELayout create(DataSet<EPGMGraphHead> graphHeads, DataSet<EPGMVertex> vertices,
    DataSet<EPGMEdge> edges) {
    Objects.requireNonNull(graphHeads, "EPGMGraphHead DataSet was null");
    Objects.requireNonNull(vertices, "EPGMVertex DataSet was null");
    Objects.requireNonNull(edges, "EPGMEdge DataSet was null");
    return new GVELayout(graphHeads, vertices, edges);
  }

  /**
   * Creates a collection layout from the given datasets indexed by label.
   *
   * @param graphHeads Mapping from label to graph head dataset
   * @param vertices Mapping from label to vertex dataset
   * @param edges Mapping from label to edge dataset
   * @return GVE layout
   */
  GVELayout create(Map<String, DataSet<EPGMGraphHead>> graphHeads,
    Map<String, DataSet<EPGMVertex>> vertices, Map<String, DataSet<EPGMEdge>> edges) {
    Objects.requireNonNull(graphHeads);
    Objects.requireNonNull(vertices);
    Objects.requireNonNull(edges);

    return new GVELayout(
      graphHeads.values().stream().reduce(DataSet::union)
        .orElseThrow(() -> new RuntimeException("Error during graph head union")),
      vertices.values().stream().reduce(DataSet::union)
        .orElseThrow(() -> new RuntimeException("Error during vertex union")),
      edges.values().stream().reduce(createEdgeDataSet(Collections.EMPTY_LIST), DataSet::union)
    );
  }
}
