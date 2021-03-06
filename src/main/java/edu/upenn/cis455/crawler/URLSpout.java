package edu.upenn.cis455.crawler;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.spout.IRichSpout;
import edu.upenn.cis.stormlite.spout.SpoutOutputCollector;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.info.URLInfo;

public class URLSpout implements IRichSpout {
	static Logger log = Logger.getLogger(URLSpout.class);

    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordSpout, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();

    /**
	 * The collector is the destination for tuples; you "emit" tuples there
	 */
    TopologyContext context;
	SpoutOutputCollector collector;
	private static AtomicInteger activeThreads; //used to check whether this sput is idle
					

    public URLSpout() {
    	URLSpout.activeThreads = new AtomicInteger();
    	log.debug("Starting URL spout");
    }
    
    public static int getActiveThreads() {
    	return URLSpout.activeThreads.get();
    }

    /**
     * Initializes the instance of the spout (note that there can be multiple
     * objects instantiated)
     */
    @SuppressWarnings("rawtypes")
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.context = context;      
        
      //  log.debug(getExecutorId() + " Starting URL Spout");					
    }

    /**
     * Shut down the spout
     */
    @Override
    public void close() {
    	
    }
    	    
    /**
     * The real work happens here, in incremental fashion.  We process and output
     * the next item(s).  They get fed to the collector, which routes them
     * to targets
     * Emit host name and url, later in the crawler bolt, we will group by host such that only one bolt task handles a host
     */
    @Override
    public void nextTuple() {
    	if(XPathCrawler.getInstance().getFrontier().isEmpty()) { //TODO: Handle Shutdown
    		if(URLSpout.getActiveThreads() == 0 && CrawlerBolt.getActiveThreads() == 0 && DocumentParserBolt.getActiveThreads() == 0 && URLFilterBolt.getActiveThreads() == 0 && 
    			ChannelMatchingBolt.getActiveThreads() == 0 && XPathCrawler.getInstance().getInFlightMessages() == 0) {
    			XPathCrawler.getInstance().shutdown(); //call shutdown
    			//System.out.println("Spout Called Shutdown");
    			return;
    		} else {
    			return; //just return and don't emit anything
    		}
    		
    	} else {
    		URLSpout.activeThreads.getAndIncrement(); //isIdle is now 1, hence this thread is not idle
        	String url = XPathCrawler.getInstance().getFrontier().dequeue();
        	if(url.startsWith("http://")) {
        		URLInfo urlInfo = new URLInfo(url);			  
        		String portString = ":" + Integer.toString(urlInfo.getPortNo());
    			  if(!url.contains(portString)) { //make sure URL is in the form http://xyz.com:80/
    				  StringBuilder newURL = new StringBuilder(url);
    	  			  int index = 7 + urlInfo.getHostName().length();
    	  			  newURL.insert(index, portString);
    				  url = newURL.toString();
    			  }
    			  
            	//log.debug(getExecutorId() + " emitting " + url);
    	        this.collector.emit(new Values<Object>(urlInfo.getHostName(), url, "http"));
    	        XPathCrawler.getInstance().incrementInflightMessages();  //signals a message is currently being routed
        	}else {
        		try {
					URL httpsUrl = new URL(url); //emit https url
					int port = httpsUrl.getPort() == -1 ? 443 : httpsUrl.getPort();
					String portString = ":" + Integer.toString(port);
					if(!url.contains(portString)) { //make sure URL is in the form http://xyz.com:443/
	    				  StringBuilder newURL = new StringBuilder(url);
	    	  			  int index = 8 + httpsUrl.getHost().length();
	    	  			  newURL.insert(index, portString);
	    				  url = newURL.toString();
	    			  }
					log.debug(getExecutorId() + " emitting " + url);
	    	        this.collector.emit(new Values<Object>(httpsUrl.getHost(), url, "https"));
	    	        XPathCrawler.getInstance().incrementInflightMessages();  //signals a message is currently being routed
				} catch (MalformedURLException e) {
					e.printStackTrace();
				}
        	}
        	
        	URLSpout.activeThreads.decrementAndGet(); //isIdle is now 0 hence this thread is idle
    	}   	
        Thread.yield();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("host", "url", "protocol"));
    }


	@Override
	public String getExecutorId() {
		
		return executorId;
	}

	@Override
	public void setRouter(IStreamRouter router) {
		this.collector.setRouter(router);
	}
}
