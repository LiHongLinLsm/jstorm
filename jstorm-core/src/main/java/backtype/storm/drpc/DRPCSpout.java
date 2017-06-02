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
package backtype.storm.drpc;

import backtype.storm.Config;
import backtype.storm.ILocalDRPC;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.DRPCRequest;
import backtype.storm.generated.DistributedRPCInvocations;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.ExtendedThreadPoolExecutor;
import backtype.storm.utils.ServiceRegistry;
import backtype.storm.utils.Utils;
import com.alibaba.jstorm.utils.JStormServerUtils;
import com.alibaba.jstorm.utils.NetWorkUtils;
import org.apache.thrift.TException;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

//该类为系统自动添加的一个spout，主要作用是与drpc类实例（即drpc服务器组件）
//通信，并且取得请求，封装returnMap。以供PrepareRequest这个系统bolt消费。
public class DRPCSpout extends BaseRichSpout {
    // ANY CHANGE TO THIS CODE MUST BE SERIALIZABLE COMPATIBLE OR THERE WILL BE PROBLEMS
    static final long serialVersionUID = 2387848310969237877L;

    public static Logger LOG = LoggerFactory.getLogger(DRPCSpout.class);

    transient SpoutOutputCollector _collector;
    //drpc服务器可能是集群。。。
    transient List<DRPCInvocationsClient> _clients;
    transient LinkedList<Future<?>> _futures = null;
    transient ExecutorService _backround = null;
    String _function;
    //serviceRegistry中hash保存有：uuid--》rpcServer实例。此处为该rpc的uuid(本地模式有效)
    //如果此值为null，则是远程drpc。
    String _local_drpc_id = null;

    private static class DRPCMessageId {
        //drpc服务器生成的requestID
        String id;
        //drpc服务器的索引，有可能有多个drpc服务器。
        int index;

        public DRPCMessageId(String id, int index) {
            this.id = id;
            this.index = index;
        }
    }

    public DRPCSpout(String function) {
        _function = function;
    }

    public DRPCSpout(String function, ILocalDRPC drpc) {
        _function = function;
        _local_drpc_id = drpc.getServiceId();
    }

    public String get_function() {
        return _function;
    }

    private class Adder implements Callable<Void> {
        private String server;
        private int port;
        private Map conf;

        public Adder(String server, int port, Map conf) {
            this.server = server;
            this.port = port;
            this.conf = conf;
        }

        @Override
        public Void call() throws Exception {
            DRPCInvocationsClient c = new DRPCInvocationsClient(conf, server, port);
            synchronized (_clients) {
                _clients.add(c);
            }
            return null;
        }
    }

    private void reconnect(final DRPCInvocationsClient c) {
        _futures.add(_backround.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                c.reconnectClient();
                return null;
            }
        }));
    }

    //主要作用1.初始化属性
    //2.连接到drpcServer:即drpc类的实例。以便fetchRequest和sendResult.
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _clients = new ArrayList<DRPCInvocationsClient>();
        if (_local_drpc_id == null) {
            _backround = new ExtendedThreadPoolExecutor(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>());
            _futures = new LinkedList<Future<?>>();

            int numTasks = context.getComponentTasks(context.getThisComponentId()).size();
            //得到当前spout中所有task中，当前进程排序后的索引。ie:此taskID排在老几。
            int index = context.getThisTaskIndex();

            int port = Utils.getInt(conf.get(Config.DRPC_INVOCATIONS_PORT));
            List<String> servers = NetWorkUtils.host2Ip((List<String>) conf.get(Config.DRPC_SERVERS));

            if (servers == null || servers.isEmpty()) {
                throw new RuntimeException("No DRPC servers configured for topology");
            }

            if (numTasks < servers.size()) {
                for (String s : servers) {
                    _futures.add(_backround.submit(new Adder(s, port, conf)));
                }
            } else {
                //负载均衡
                int i = index % servers.size();
                _futures.add(_backround.submit(new Adder(servers.get(i), port, conf)));
            }
        }

    }

    @Override
    public void close() {
        for (DRPCInvocationsClient client : _clients) {
            client.close();
        }
    }

    @Override
    public void nextTuple() {
        boolean gotRequest = false;
        //集群模式
        if (_local_drpc_id == null) {
            int size = 0;
            synchronized (_clients) {
                size = _clients.size(); // This will only ever grow, so no need to worry about falling off the end
            }
            for (int i = 0; i < size; i++) {
                DRPCInvocationsClient client;
                synchronized (_clients) {
                    client = _clients.get(i);
                }
                if (!client.isConnected()) {
                    continue;
                }
                try {
                    DRPCRequest req = client.fetchRequest(_function);
                    if (req.get_request_id().length() > 0) {
                        Map returnInfo = new HashMap();
                        returnInfo.put("id", req.get_request_id());
                        returnInfo.put("host", client.getHost());
                        returnInfo.put("port", client.getPort());
                        gotRequest = true;
                        _collector.emit(new Values(req.get_func_args(), JSONValue.toJSONString(returnInfo)), new DRPCMessageId(req.get_request_id(), i));
                        break;
                    }
                } catch (AuthorizationException aze) {
                    reconnect(client);
                    LOG.error("Not authorized to fetch DRPC request from DRPC server", aze);
                } catch (TException e) {
                    reconnect(client);
                    LOG.error("Failed to fetch DRPC request from DRPC server", e);
                } catch (Exception e) {
                    LOG.error("Failed to fetch DRPC request from DRPC server", e);
                }
            }
            JStormServerUtils.checkFutures(_futures);
        //本地模式。
        } else {
            //名字没取好~~~
            DistributedRPCInvocations.Iface drpc = (DistributedRPCInvocations.Iface) ServiceRegistry.getService(_local_drpc_id);
            if (drpc != null) { // can happen during shutdown of drpc while topology is still up
                try {
                    DRPCRequest req = drpc.fetchRequest(_function);
                    if (req.get_request_id().length() > 0) {
                        Map returnInfo = new HashMap();
                        returnInfo.put("id", req.get_request_id());
                        returnInfo.put("host", _local_drpc_id);
                        returnInfo.put("port", 0);
                        gotRequest = true;
                        _collector.emit(new Values(req.get_func_args(), JSONValue.toJSONString(returnInfo)), new DRPCMessageId(req.get_request_id(), 0));
                    }
                } catch (AuthorizationException aze) {
                    throw new RuntimeException(aze);
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if (!gotRequest) {
            Utils.sleep(1);
        }
    }

    @Override
    public void ack(Object msgId) {
    }

    @Override
    public void fail(Object msgId) {
        DRPCMessageId did = (DRPCMessageId) msgId;
        DistributedRPCInvocations.Iface client;

        if (_local_drpc_id == null) {
            client = _clients.get(did.index);
        } else {
            client = (DistributedRPCInvocations.Iface) ServiceRegistry.getService(_local_drpc_id);
        }
        try {
            client.failRequest(did.id);
        } catch (AuthorizationException aze) {
            LOG.error("Not authorized to failREquest from DRPC server", aze);
        } catch (TException e) {
            LOG.error("Failed to fail request", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("args", "return-info"));
    }
}
