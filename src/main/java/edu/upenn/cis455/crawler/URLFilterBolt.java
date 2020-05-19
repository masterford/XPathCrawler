package edu.upenn.cis455.crawler;

import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;

public class URLFilterBolt implements IRichBolt {

	
	static Logger log = Logger.getLogger(URLFilterBolt.class);
	
	Fields schema = new Fields(); //not emitting anything
	
    
    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
    /**
     * This is where we send our output stream
     */
    private OutputCollector collector;
    private static AtomicInteger activeThreads;
    
    public URLFilterBolt() {
    	activeThreads = new AtomicInteger();
    }
    
    public static int getActiveThreads() {
    	return URLFilterBolt.activeThreads.get();
    }
    
    /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
        
    /**
     * Process a tuple received from the stream, incrementing our
     * counter and outputting a result
     */
    @Override
    public void execute(Tuple input) {
    	XPathCrawler.getInstance().decrementInflightMessages(); //message has been routed to final destination
    	URLFilterBolt.activeThreads.getAndIncrement();
    	//Check if maxFileCount reached, if so send shutdown
    	String url = input.getStringByField("url");
    	if (url == null) {
    		return;
    	}
    	//System.out.println(getActiveThreads());
    	HashSet<String> seenURLs = (HashSet<String>) XPathCrawler.getInstance().getSeenURLs();
    	if(!seenURLs.contains(url)) { //add to crawl queue if not seen
    		seenURLs.add(url);
    		XPathCrawler.getInstance().getFrontier().enqueue(url);
    	}
    	URLFilterBolt.activeThreads.getAndDecrement();
    }

    /**
     * Shutdown, just frees memory
     */
    @Override
    public void cleanup() {
    	
    	//wordCounter.clear();
    }

    /**
     * Lets the downstream operators know our schema
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(schema);
    }

    /**
     * Used for debug purposes, shows our exeuctor/operator's unique ID
     */
	@Override
	public String getExecutorId() {
		return executorId;
	}

	/**
	 * Called during topology setup, sets the router to the next
	 * bolt
	 */
	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}

	/**
	 * The fields (schema) of our output stream
	 */
	@Override
	public Fields getSchema() {
		return schema;
	}
 }
