package com.chenshun.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * User: chenshun131 <p />
 * Time: 18/5/13 12:31  <p />
 * Version: V1.0  <p />
 * Description:  <p />
 */
public class WordCountTopology {

    /**
     * Spout 主要用于从数据员获取数据
     * 此处做了一个简化，不从外部数据源获取数据而是直接发生一些句子
     */
    public static class RandomSentenceSpout extends BaseRichSpout {

        private static final long serialVersionUID = 255424280741882160L;

        private static final Logger LOGGER = LoggerFactory.getLogger(RandomSentenceSpout.class);

        private SpoutOutputCollector collector;

        private Random random;

        /**
         * open 方法，是堆 spout 尽心初始化，比如创建一个线程池或者创建一个数据库连接池，或者构造一个 httpclient
         *
         * @param conf
         * @param context
         * @param collector
         */
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            // open 方法初始化的时候，会传入 SpoutOutputCollector，这个 SpoutOutputCollector 就是用来发射数据出去
            this.collector = collector;
            this.random = new Random();
        }

        /**
         * spout 会运行在 task 中，某个 workr 进程的某个 executor 线程内部的某个 task 中，那个
         * task 会负责去不断的无限循环调用 nextTuple() 方法，只要无限循环就可以不断发射最新的数据出去，形成一个数据流
         */
        public void nextTuple() {
            Utils.sleep(100);
            String[] sentences = new String[]{"the cow jumped over the moon",
                    "an apple a day keeps the doctor away",
                    "four score and seven years ago",
                    "snow white and the seven dwarfs",
                    "i am at two with nature"};
            String sentence = sentences[random.nextInt(sentences.length)];
            LOGGER.info("【发射句子】sentence=" + sentence);
            // 这个 Values 可以认为是构建的 tuple
            // tuple 是最小的数据单元，无限个 tuple 组成的流就是一个 stream
            collector.emit(new Values(sentence));
        }

        /**
         * 定义发射出去的每个 tuple 中 field 的名称
         *
         * @param declarer
         */
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("setence"));
        }
    }

    /**
     * bolt 发送到 worker进程的某个 executor线程的 task 里面去运行
     */
    public static class SplitSentence extends BaseRichBolt {

        private static final long serialVersionUID = 6601857039647362803L;

        private OutputCollector collector;

        /**
         * 对于 bolt 来说，第一个方法就是 prepare
         *
         * @param stormConf
         * @param context
         * @param collector
         */
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        /**
         * 每次接收打一个数据之后，就会交给 execute 方法来之性
         *
         * @param input
         */
        public void execute(Tuple input) {
            String setence = input.getStringByField("setence");
            String[] words = setence.split(" ");
            for (String word : words) {
                collector.emit(new Values(word));
            }
        }

        /**
         * 发射出去的 tuple，每个 field 的名称
         *
         * @param declarer
         */
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class WordCount extends BaseRichBolt {

        private static final long serialVersionUID = -117518143537210996L;

        private static final Logger LOGGER = LoggerFactory.getLogger(WordCount.class);

        private OutputCollector collector;

        private Map<String, Long> wordCounts = new HashMap<String, Long>();

        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        public void execute(Tuple input) {
            String word = input.getStringByField("word");
            Long count = wordCounts.get(word);
            if (count == null) {
                wordCounts.put(word, 0L);
                count = wordCounts.get(word);
            }
            count++;
            LOGGER.info("【单词计数】{}出现的次数是{}", word, count);
            collector.emit(new Values(word, count));
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static void main(String[] args) {
        // 将 spout 和 bolt 组合起来，构成一个拓扑
        TopologyBuilder builder = new TopologyBuilder();
        // 这里的第一个参数的意思，就是给这个spout设置一个名字
        // 第二个参数的意思，就是创建一个spout的对象
        // 第三个参数的意思，就是设置spout的executor有几个
        builder.setSpout("RandomSentence", new RandomSentenceSpout(), 2);
        builder.setBolt("SplitSretence", new SplitSentence(), 5)
                .setNumTasks(10)
                .shuffleGrouping("RandomSentence");
        // 相同的单词，从 SplitSentence 发射出来时，一定会进入到下游的指定的同一个task中
        // 只有这样子，才能准确的统计出每个单词的数量
        // 比如你有个单词，hello，下游task1接收到3个hello，task2接收到2个hello
        // 5个hello，全都进入一个task
        builder.setBolt("WordCount", new WordCount(), 10)
                .setNumTasks(20)
                .fieldsGrouping("SplitSretence", new Fields("word"));

        Config config = new Config();
        // 在命令执行，打算提交到 storm 集群上去
        if (args != null && args.length > 0) {
            config.setNumWorkers(3);
            try {
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            // 没有接收到到参数说明在开发工具中运行
            config.setMaxTaskParallelism(20);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("WordCountTopology", config, builder.createTopology());
            Utils.sleep(60000);
            cluster.shutdown();
        }
    }

}
