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
package com.alipay.dw.jstorm.example.drpc;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.LoadConf;

import java.util.*;

/**
 * This is a good example of doing complex Distributed RPC on top of Storm. This
 * program creates a topology that can compute the reach for any URL on Twitter
 * in realtime by parallelizing the whole computation.
 *
 * Reach is the number of unique people exposed to a URL on Twitter. To compute
 * reach, you have to get all the people who tweeted the URL, get all the
 * followers of all those people, unique that set of followers, and then count
 * the unique set. It's an intense computation that can involve thousands of
 * database calls and tens of millions of follower records.
 *
 * This Storm topology does every piece of that computation in parallel, turning
 * what would be a computation that takes minutes on a single machine into one
 * that takes just a couple seconds.
 *
 * For the purposes of demonstration, this topology replaces the use of actual
 * DBs with in-memory hashmaps.
 *
 * See https://github.com/nathanmarz/storm/wiki/Distributed-RPC for more
 * information on Distributed RPC.
 */

/**
 * 该类笔记：
 *
 * 其中，DRPC Topology由1个DRPCSpout、1个Prepare-Request Bolt、若干个User Bolts
 *   （即用户通过LinearDRPCTopologyBuilder添加的Bolts）、1个JoinResult Bolt和1个ReturnResults Bolt组成。
 *   除了User Bolts以外，其他的都是由LinearDRPCTopologyBuilder内置添加到Topology中的。
 *   接下来，我们从数据流的流动关系来 看，这些Spout和Bolts是如何工作的：

 1. DRPCSpout中维护了若干个DRPCInvocationsClient，
 通过fetchRequest方法从DRPC Server读取需要提交到Topology中计算的RPC请求，
 然后发射一条数据流给Prepare-Request Bolt：<”args”, ‘”return-info”>，
 其中args表示RPC请求的参数，而return-info中则包含了发起这次RPC请求的RPC Server信息（host、port、request id），
 用于后续在ReturnResults Bolt中返回计算结果时使用。

 2. Prepare-Request Bolt接收到数据流后，会新生成三条数据流：

 <”request”, ”args”>：发给用户定义的User Bolts，提取args后进行DRPC的实际计算过程；
 <”request”, ”return-info”>：发给JoinResult Bolt，用于和User Bolts的计算结果做join以后将结果返回给客户端；
 <”request”>：在用户自定义Bolts实现了FinishedCallback接口的情况下，作为ID流发给用户定义的最后一级Bolt，用于判断batch是否处理完成。

 3. User Bolts按照用户定义的计算逻辑，以及RPC调用的参数args，进行业务计算，
 并最终输出一条数据流给JoinResult Bolt：<”request”, ”result”>。

 4. JoinResult Bolt将上游发来的<”request”, ”return-info”>和<”request”, ”result”>两条数据流做join，
 然后输出一条新的数据流给ReturnResults Bolt： <”result”, ”return-info”>。

 5. ReturnResults Bolt接收到数据流后，从return-info中提取出host、port、request id，
 根据host和port生成DRPCInvocationsClient对象，并调用result方法将request id及result返回给DRPC Server，
 如果result方法调用成功，则对tuple进行ack，否则对tuple进行fail，并最终在DRPCSpout中检测到tuple 失败后，
 调用failRequest方法通知DRPC Server该RPC请求执行失败。
 */

public class ReachTopology {
    public final static String TOPOLOGY_NAME = "reach";
    
    public static Map<String, List<String>> TWEETERS_DB = new HashMap<String, List<String>>() {
        {
            put("foo.com/blog/1", Arrays.asList("sally", "bob", "tim", "george", "nathan"));
            put("engineering.twitter.com/blog/5", Arrays.asList("adam", "david", "sally", "nathan"));
            put("tech.backtype.com/blog/123", Arrays.asList("tim", "mike", "john"));
        }
    };
    
    public static Map<String, List<String>> FOLLOWERS_DB = new HashMap<String, List<String>>() {
        {
            put("sally", Arrays.asList("bob", "tim", "alice", "adam", "jim", "chris", "jai"));
            put("bob", Arrays.asList("sally", "nathan", "jim", "mary", "david", "vivian"));
            put("tim", Arrays.asList("alex"));
            put("nathan", Arrays.asList("sally", "bob", "adam", "harry", "chris", "vivian", "emily", "jordan"));
            put("adam", Arrays.asList("david", "carissa"));
            put("mike", Arrays.asList("john", "bob"));
            put("john", Arrays.asList("alice", "nathan", "jim", "mike", "bob"));
        }
    };
    
    public static class GetTweeters extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            Object id = tuple.getValue(0);
            String url = tuple.getString(1);
            List<String> tweeters = TWEETERS_DB.get(url);
            if (tweeters != null) {
                for (String tweeter : tweeters) {
                    collector.emit(new Values(id, tweeter));
                }
            }
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "tweeter"));
        }
    }
    
    public static class GetFollowers extends BaseBasicBolt {
        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            Object id = tuple.getValue(0);
            String tweeter = tuple.getString(1);
            List<String> followers = FOLLOWERS_DB.get(tweeter);
            if (followers != null) {
                for (String follower : followers) {
                    collector.emit(new Values(id, follower));
                }
            }
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "follower"));
        }
    }
    
    public static class PartialUniquer extends BaseBatchBolt {
        BatchOutputCollector _collector;
        Object               _id;
        Set<String>          _followers = new HashSet<String>();
        
        @Override
        public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
            _collector = collector;
            _id = id;
        }
        
        @Override
        public void execute(Tuple tuple) {
            _followers.add(tuple.getString(1));
        }
        
        @Override
        public void finishBatch() {
            _collector.emit(new Values(_id, _followers.size()));
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "partial-count"));
        }
    }
    
    public static class CountAggregator extends BaseBatchBolt {
        BatchOutputCollector _collector;
        Object               _id;
        int                  _count = 0;
        
        @Override
        public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
            _collector = collector;
            _id = id;
        }
        
        @Override
        public void execute(Tuple tuple) {
            _count += tuple.getInteger(1);
        }
        
        @Override
        public void finishBatch() {
            _collector.emit(new Values(_id, _count));
        }
        
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "reach"));
        }
    }
    
    public static LinearDRPCTopologyBuilder construct() {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder(TOPOLOGY_NAME);
        builder.addBolt(new GetTweeters(), 1);
        builder.addBolt(new GetFollowers(), 1).shuffleGrouping();
        builder.addBolt(new PartialUniquer(), 1).fieldsGrouping(new Fields("id", "follower"));
        builder.addBolt(new CountAggregator(), 1).fieldsGrouping(new Fields("id"));
        return builder;
    }
    
    public static void main(String[] args) throws Exception {

        LinearDRPCTopologyBuilder builder = construct();
        
        Config conf = new Config();
        conf.setNumWorkers(6);
        if (args.length != 0) {
            
            try {
                Map yamlConf = LoadConf.LoadYaml(args[0]);
                if (yamlConf != null) {
                    conf.putAll(yamlConf);
                }
            } catch (Exception e) {
                System.out.println("Input " + args[0] + " isn't one yaml ");
            }
            
            StormSubmitter.submitTopology(TOPOLOGY_NAME, conf, builder.createRemoteTopology());
        } else {
            
            conf.setMaxTaskParallelism(3);
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createLocalTopology(drpc));
            
            JStormUtils.sleepMs(10000);
            
            String[] urlsToTry = new String[] { "foo.com/blog/1", "engineering.twitter.com/blog/5", "notaurl.com" };
            for (String url : urlsToTry) {
                System.out.println("Reach of " + url + ": " + drpc.execute(TOPOLOGY_NAME, url));
            }
            
            cluster.shutdown();
            drpc.shutdown();
        }
    }
}
