package com.scott.eshop.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * 单词计数的拓扑
 * @ClassName WordCountTopology
 * @Description
 * @Author 47980
 * @Date 2020/5/3 16:06
 * @Version V_1.0
 **/
public class WordCountTopology {

    /**
     * spout
     *
     * spout, 继承一个基类，实现接口，这里主要时负责从数据源获取数据
     *
     * 这里简化，不从外部数据源获取数据，只内部不断发送一些句子
     *
     */
    public static class RandomSentenceSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;

        private Random random;

        /**
         * open 方法
         *
         * 对spout进行初始化的
         *
         * 比方说创建线程池，或者创建一个数据库连接池，或者httpclient
         * @param map
         * @param topologyContext
         * @param spoutOutputCollector
         */
        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            // 在open方法初始化的时候，会传入一个东西，叫做spoutOutputCollector
            // 这个 SpoutOutputCollector 就是发送数据出去的
            this.collector = spoutOutputCollector;
            // 构造一个随机数生产对象
            this.random = new Random();
        }

        /**
         * nextTuple方法
         *
         * spout类，最总会放在task中，某个worker的executor内部的task中
         *
         * 那么task回去负责不断的无线循环调用nextTuple()方法
         * 只要无线循环，就可以不断发送最新的数据，形成一个数据流
         */
        @Override
        public void nextTuple() {
            Utils.sleep(100L);
            String[] sentences = new String[]{"the cow jumped over the moon","an apple a day keeps the doctor away","four score and seven years ago","snow white and the seven dwarfs","i am at two with nature"};
            String sentence = sentences[random.nextInt(sentences.length)];
            System.err.println("【发射句子】, sentence="+sentence);
            // 构建一个tuple
            collector.emit(new Values(sentence));
        }

        /**
         * declareOutputFields
         *
         * 定义发送出去的每个tuple中的每个field的名称是什么
         * @param outputFieldsDeclarer
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("sentence"));
        }
    }

    /**
     * 写一个bolt，直接继承一个BaseRichBolt基类
     *
     * 实现所有方法即可，每个bolt代码，同样是发送到某个worker的executor内部的task中
     */
    public static class SplitSentence extends BaseRichBolt {

        private OutputCollector collector;

        /**
         * 对于bolt来说，第一个是prepare
         *
         * OutputCollector，也就是这个bolt的发射器。把代码发射出去。
         * @param map
         * @param topologyContext
         * @param outputCollector
         */
        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        /**
         * execute方法
         *
         * 每次接收到一条数据，都会交给这个executor放来去执行
         * @param tuple
         */
        @Override
        public void execute(Tuple tuple) {
            String sentence = tuple.getStringByField("sentence");
            String[] words = sentence.split(" ");
            for (String word: words
                 ) {
                collector.emit(new Values(word));
            }
        }

        /**
         * 定义发送出去的每个tuple中的每个field的名称是什么
         * @param outputFieldsDeclarer
         */
        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }

    /**
     * 计算word出现的次数
     */
    public static class WordCount extends BaseRichBolt {
        private OutputCollector collector;
        private Map<String, Long> wordCounts = new HashMap<>();

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            String word = tuple.getStringByField("word");
            Long count = wordCounts.get(word);
            if (count == null) {
                count = 0L;
            }
            count++;

            wordCounts.put(word, count);

            System.err.println("【单词计数】，出现的单词："+word+"，出现的次数："+count);

            collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word", "count"));
        }
    }

    public static void main(String[] args) {
        // 将spout和bolts组合起来，构建成一个拓扑
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        // params1: 设置名字
        // params2: 创建spout对象
        // params3: 设置executor有几个
        topologyBuilder.setSpout("RandomSentence", new RandomSentenceSpout(), 2);

        topologyBuilder.setBolt("SplitSentence", new SplitSentence(), 5)
                .setNumTasks(10)
                .shuffleGrouping("RandomSentence");
        // 这里的策略使用fields-grouping，从SplitSentence发射出来时，一定会进入到下游的指定的同一个task中去。
        // 只有这样，才能准确统计出每个单词的数量
        // 比如有一个hello，下游task1收到3次，task2收到2次，就不对了。
        topologyBuilder.setBolt("WordCount", new WordCount(), 10)
                .setNumTasks(20)
                .fieldsGrouping("SplitSentence", new Fields("word"));

        Config config = new Config();

        // 说明时命令行执行，打算提交到storm集群上去
        if (args != null && args.length > 0) {
            config.setNumWorkers(3);
            try {
                StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            // 说明是在本地运行
            config.setNumWorkers(20);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("WordCountTopology", config, topologyBuilder.createTopology());

            Utils.sleep(10000);

            cluster.shutdown();
        }
    }
}
