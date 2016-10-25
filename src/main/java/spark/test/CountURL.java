package spark.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Lists;

import scala.Tuple2;

public class CountURL{
	    @SuppressWarnings("serial")
		public static void main(String[] args) {
	        //设置匹配模式，以空格分隔
	    	final Pattern CHANGELINE = Pattern.compile("\r\n");
	        //接收数据的地址和端口
	        String zkQuorum = "192.168.144.227:2181";
	        //话题所在的组
	        String group = "1";
	        //话题名称以“，”分隔
	        String topics = "test";
	        //每个话题的分片数
	        int numThreads = 2;        
	        SparkConf sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]");
	        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(10000));
//	        jssc.checkpoint("checkpoint"); //设置检查点
	        //存放话题跟分片的映射关系
	        Map<String, Integer> topicmap = new HashMap<>();
	        String[] topicsArr = topics.split(",");
	        int n = topicsArr.length;
	        for(int i=0;i<n;i++){
	            topicmap.put(topicsArr[i], numThreads);
	        }
	        //从Kafka中获取数据转换成RDD
	        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(jssc, zkQuorum, group, topicmap);
	        
	      //从话题中过滤所需数据
	        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
	        	@Override
	            public Iterable<String> call(Tuple2<String, String> arg0)
	                    throws Exception {
	            	return Lists.newArrayList(CHANGELINE.split(arg0._2));
	            }
	        });
	        
	        //对其中的单词进行统计
	        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
	        	new PairFunction<String, String, Integer>() {
	                @Override
	                public Tuple2<String, Integer> call(String s) {
	                	String str = "";
	                	if(s.indexOf("GET")!=-1 && s.indexOf("HTTP/1.1")!=-1){
	                		str = s.substring(s.indexOf("GET")+3,s.indexOf("HTTP/1.1")).trim();
	                		return new Tuple2<String, Integer>("http://www.chinanews.com"+str, 1);
	                	}else if(s.indexOf("HEAD")!=-1 && s.indexOf("HTTP/1.1")!=-1){
	                		str = s.substring(s.indexOf("HEAD")+4,s.indexOf("HTTP/1.1")).trim();
	                		return new Tuple2<String, Integer>("http://www.chinanews.com"+str, 1);
	                	}else if(s.indexOf("GET")!=-1 && s.indexOf("HTTP/1.0")!=-1){
	                		str = s.substring(s.indexOf("GET")+4,s.indexOf("HTTP/1.0")).trim();
	                		return new Tuple2<String, Integer>("http://www.chinanews.com"+str, 1);
	                	}else if(s.indexOf("HEAD")!=-1 && s.indexOf("HTTP/1.0")!=-1){
	                		str = s.substring(s.indexOf("HEAD")+4,s.indexOf("HTTP/1.0")).trim();
	                		return new Tuple2<String, Integer>("http://www.chinanews.com"+str, 1);
	                	}else{
	                		return new Tuple2<String, Integer>(s,1);
	                	}
	                }
	            }
	        ).reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer i1, Integer i2) {
                  return i1 + i2;
                }
            });
	        //调换key、value
	        JavaPairDStream<Integer, String> swappedCounts = wordCounts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>(){
	        	@Override
	        	public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
	                return item.swap();
	            }
	        });
	        //根据key排序
	        JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transformToPair(
	        	new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {
	        		public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> in) throws Exception {
	        			return in.sortByKey(false);
	        		}
	        	}
	        );
	        //调换key、value
	        JavaPairDStream<String, Integer> resultCounts = sortedCounts.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>(){
	        	@Override
	        	public Tuple2<String, Integer> call(Tuple2<Integer, String> item) throws Exception {
	                return item.swap();
	            }
	        });
	        
	        //打印结果
	        resultCounts.print(200);
	       
	        jssc.start();
	        jssc.awaitTermination();

	    }
        
	    //存入关系数据库的方法
		public static void importData(Iterator<Tuple2<String, Integer>> iterator)
				throws Exception {
	    	Connection conn = null; 
	    	Statement st = null; 			
			try { 
			   Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");//加载驱动类 
			   conn = DriverManager.getConnection("jdbc:microsoft:sqlserver://<server_name>:<1433>", "name","pwd"); 
			   conn.setAutoCommit(false); 
			   st = conn.createStatement(); 			  			   
			   while(iterator.hasNext()){
				  Tuple2<String, Integer> tuple =  iterator.next();
				    String sqlStr = "INSERT INTO tableName VALUES('" + tuple._1 + "',"+tuple._2+")";//向数据库中插入数据 
				    st.executeUpdate(sqlStr); 
			   }				   
			   conn.commit(); 
			} catch (Exception e) { 
			   e.printStackTrace(); 
			} finally {//释放数据库的资源 
			   try { 
				    if (st != null) 
				     st.close(); 
				    if(conn != null && !conn.isClosed()){ 
				     conn.close(); 
				    } 
			   } catch (SQLException e) { 
				  	e.printStackTrace(); 
			   } 
			}
	   }
}
