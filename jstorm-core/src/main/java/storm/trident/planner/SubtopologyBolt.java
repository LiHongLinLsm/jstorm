/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.trident.planner;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DirectedSubgraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import storm.trident.planner.processor.TridentContext;
import storm.trident.state.State;
import storm.trident.topology.BatchInfo;
import storm.trident.topology.ITridentBatchBolt;
import storm.trident.tuple.TridentTuple;
import storm.trident.tuple.TridentTuple.Factory;
import storm.trident.tuple.TridentTupleView.ProjectionFactory;
import storm.trident.tuple.TridentTupleView.RootFactory;
import storm.trident.util.TridentUtils;

import java.util.*;

// TODO: parameterizing it like this with everything might be a high deserialization cost if there's lots of tasks?
// TODO: memory problems?
// TODO: can avoid these problems by adding a boltfactory abstraction, so that boltfactory is deserialized once
//   bolt factory -> returns coordinatedbolt per task, but deserializes the batch bolt one time and caches


/**
 * 在tridentBoltExecutor中执行。
 */
public class SubtopologyBolt implements ITridentBatchBolt {
    //整个top对应的有向图
    DirectedGraph _graph;
    //该bolt包含的node。是graph的子集。
    Set<Node> _nodes;
    //该bolt可以接受多个类型的流。key:streamId.
    Map<String, InitialReceiver> _roots = new HashMap();

    Map<Node, Factory> _outputFactories = new HashMap();

    //key:Node组索引，val:对应的一些了节点，是有顺序的。  比如，在有drpc的top中，查询和保存状态的节点可能放在一个bolt中执行。
    Map<String, List<TridentProcessor>> _myTopologicallyOrdered = new HashMap();
    //key:node,val:groupId.
    Map<Node, String> _batchGroups;

    // given processornodes and static state nodes
    public SubtopologyBolt(DirectedGraph graph, Set<Node> nodes, Map<Node, String> batchGroups) {
        _nodes = nodes;
        _graph = graph;
        _batchGroups = batchGroups;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector batchCollector) {
        int thisComponentNumTasks = context.getComponentTasks(context.getThisComponentId()).size();
        for (Node n : _nodes) {
            if (n.stateInfo != null) {
                State s = n.stateInfo.spec.stateFactory.makeState(conf, context, context.getThisTaskIndex(), thisComponentNumTasks);
                context.setTaskData(n.stateInfo.id, s);
            }
        }
        DirectedSubgraph<Node, Object> subgraph = new DirectedSubgraph(_graph, _nodes, null);
        TopologicalOrderIterator it = new TopologicalOrderIterator<Node, Object>(subgraph);
        int stateIndex = 0;
        while (it.hasNext()) {
            Node n = (Node) it.next();
            if (n instanceof ProcessorNode) {
                ProcessorNode pn = (ProcessorNode) n;
                String batchGroup = _batchGroups.get(n);
                if (!_myTopologicallyOrdered.containsKey(batchGroup)) {
                    _myTopologicallyOrdered.put(batchGroup, new ArrayList());
                }
                _myTopologicallyOrdered.get(batchGroup).add(pn.processor);
                List<String> parentStreams = new ArrayList();
                List<Factory> parentFactories = new ArrayList();
                for (Node p : TridentUtils.getParents(_graph, n)) {
                    parentStreams.add(p.streamId);
                    if (_nodes.contains(p)) {
                        parentFactories.add(_outputFactories.get(p));
                    } else {
                        if (!_roots.containsKey(p.streamId)) {
                            _roots.put(p.streamId, new InitialReceiver(p.streamId, getSourceOutputFields(context, p.streamId)));
                        }
                        _roots.get(p.streamId).addReceiver(pn.processor);
                        parentFactories.add(_roots.get(p.streamId).getOutputFactory());
                    }
                }
                List<TupleReceiver> targets = new ArrayList();
                boolean outgoingNode = false;
                for (Node cn : TridentUtils.getChildren(_graph, n)) {
                    if (_nodes.contains(cn)) {
                        targets.add(((ProcessorNode) cn).processor);
                    } else {
                        outgoingNode = true;
                    }
                }
                if (outgoingNode) {
                    targets.add(new BridgeReceiver(batchCollector));
                }
                
                TridentContext triContext = new TridentContext(
                        pn.selfOutFields,
                        parentFactories,
                        parentStreams,
                        targets,
                        pn.streamId,
                        stateIndex,
                        batchCollector
                        );
                pn.processor.prepare(conf, context, triContext);
                _outputFactories.put(n, pn.processor.getOutputFactory());
            }
            stateIndex++;
        }
        // TODO: get prepared one time into executor data... need to avoid the ser/deser
        // for each task (probably need storm to support boltfactory)
    }

    private Fields getSourceOutputFields(TopologyContext context, String sourceStream) {
        for (GlobalStreamId g : context.getThisSources().keySet()) {
            if (g.get_streamId().equals(sourceStream)) {
                return context.getComponentOutputFields(g);
            }
        }
        throw new RuntimeException("Could not find fields for source stream " + sourceStream);
    }

    @Override
    public void execute(BatchInfo batchInfo, Tuple tuple) {
        String sourceStream = tuple.getSourceStreamId();
        InitialReceiver ir = _roots.get(sourceStream);
        if (ir == null) {
            throw new RuntimeException("Received unexpected tuple " + tuple.toString());
        }
        ir.receive((ProcessorContext) batchInfo.state, tuple);
    }

    @Override
    public void finishBatch(BatchInfo batchInfo) {
        for (TridentProcessor p : _myTopologicallyOrdered.get(batchInfo.batchGroup)) {
            p.finishBatch((ProcessorContext) batchInfo.state);
        }
    }

    @Override
    public Object initBatchState(String batchGroup, Object batchId) {
        ProcessorContext ret = new ProcessorContext(batchId, new Object[_nodes.size()]);
        for (TridentProcessor p : _myTopologicallyOrdered.get(batchGroup)) {
            p.startBatch(ret);
        }
        return ret;
    }

    @Override
    public void cleanup() {
        for (String bg : _myTopologicallyOrdered.keySet()) {
            for (TridentProcessor p : _myTopologicallyOrdered.get(bg)) {
                p.cleanup();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        for (Node n : _nodes) {
            declarer.declareStream(n.streamId, TridentUtils.fieldsConcat(new Fields("$batchId"), n.allOutputFields));
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    /**
     * 为输入做准备。
     */
    protected static class InitialReceiver {
        //project后的消息，依次通过此prcocessors.
        List<TridentProcessor> _receivers = new ArrayList<>();
        //用于将tuple转化成tridentTuple.
        RootFactory _factory;
        //其输入即是rootFactory的输出。
        ProjectionFactory _project;
        //输入消息的streamId.
        String _stream;

        public InitialReceiver(String stream, Fields allFields) {
            // TODO: don't want to project for non-batch bolts...???
            // how to distinguish "batch" streams from non-batch streams?
            _stream = stream;
            _factory = new RootFactory(allFields);
            List<String> projected = new ArrayList(allFields.toList());
            /**
             * procssorContext中保留了batchId，此处移除掉。
             */
            projected.remove(0);
            _project = new ProjectionFactory(_factory, new Fields(projected));
        }

        public void receive(ProcessorContext context, Tuple tuple) {
            TridentTuple t = _project.create(_factory.create(tuple));
            for (TridentProcessor r : _receivers) {
                r.execute(context, _stream, t);
            }
        }

        public void addReceiver(TridentProcessor p) {
            _receivers.add(p);
        }

        public Factory getOutputFactory() {
            return _project;
        }
    }
}
