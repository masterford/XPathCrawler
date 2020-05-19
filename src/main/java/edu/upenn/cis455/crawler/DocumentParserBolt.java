package edu.upenn.cis455.crawler;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.info.URLInfo;
import edu.upenn.cis455.storage.DocVal;

public class DocumentParserBolt implements IRichBolt{
	static Logger log = Logger.getLogger(DocumentParserBolt.class);
	
	Fields schema = new Fields("url", "document", "toMatch");
	
    
    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
    /**
     * This is where we send our output stream
     */
    private OutputCollector collector;
    private final int HTTPS_PORT = 443; //default HTTPS port
    private static AtomicInteger activeThreads;
    
    public DocumentParserBolt() {
    	DocumentParserBolt.activeThreads = new AtomicInteger();
    }
    
    public static int getActiveThreads() {
    	return DocumentParserBolt.activeThreads.get();
    }
    
    /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    
    private String normalize(String oldURL, URLInfo urlInfo, String redirectURL) {
  	  StringBuilder newURL;
  	  if(redirectURL.startsWith("http")) { //already absolute
  		   newURL = new StringBuilder(redirectURL);
  		   //append port number
  		   String portString = ":" + Integer.toString(urlInfo.getPortNo());
  		   if(!redirectURL.contains(portString)){ //append port
  			   int index = redirectURL.startsWith("https://") ? 8 : 7;
  			   index += urlInfo.getHostName().length();
  			   newURL.insert(index, portString);
  		   }		   		  	   
  	  }else { //relative URL
  		   String temp = oldURL.substring(0, oldURL.lastIndexOf("/"));
  		   newURL = new StringBuilder(temp);
  		   newURL.append(redirectURL);
  	  } 
  	  return newURL.toString();
    }
    
    private String normalizeHttps(String oldURL, URL url, String redirectURL) {
  	  StringBuilder newURL;
  	  if(redirectURL.startsWith("http")) { //already absolute
  		   newURL = new StringBuilder(redirectURL);
  		   //append port number
  		   int port = url.getPort() == -1 ? HTTPS_PORT : url.getPort();
  		   String portString = ":" + Integer.toString(port);
  		   if(!redirectURL.contains(portString)){ //append port
  			   int index = redirectURL.startsWith("https://") ? 8 : 7;
  			   index += url.getHost().length();
  			   newURL.insert(index, portString);
  		   }		   		  	   
  	  }else { //relative URL
  		   String temp = oldURL.substring(0, oldURL.lastIndexOf("/"));
  		   newURL = new StringBuilder(temp);
  		   newURL.append(redirectURL);
  	  } 
  	  return newURL.toString();
    }

    /**
     * Process a tuple received from the stream, incrementing our
     * counter and outputting a result
     */
    @Override
    public void execute(Tuple input) {
    	XPathCrawler.getInstance().decrementInflightMessages();
    	//Check if maxFileCount reached, if so send shutdown
    	XPathCrawler crawler = XPathCrawler.getInstance();
    	if(crawler.getFileCount().get() >= crawler.getMaxFileNum()) {
    		crawler.shutdown(); //send shutdown signal
    		System.out.println("DocumentParser Bolt Called Shutdown");
    		return; //TODO: change?
    	}
    	DocumentParserBolt.activeThreads.getAndIncrement();
    	String url = input.getStringByField("url");
    	DocVal doc = (DocVal) input.getObjectByField("document");
    	if(input.getStringByField("toStore") != null && input.getStringByField("toStore").equals("true")) {
    		XPathCrawler.getInstance().getDB().addDocInfo(url, doc); //store document in DB
    		XPathCrawler.getInstance().getFileCount().getAndIncrement(); //increment file count
    	}
    
    	XPathCrawler.getInstance().incrementInflightMessages();
    	collector.emit(new Values<Object>(url, doc, "true"));
    	
    	if(doc.getContentType() != null && (doc.getContentType().contains("text/html") || doc.getContentType().endsWith(".html"))) { //parse html doc
    		URL httpsUrl = null;
    		try {
    			httpsUrl = new URL(url);
    		} catch (MalformedURLException e) {
    			e.printStackTrace();
    		}
    		String html = new String(doc.getBody()); 
    		Document jdoc = Jsoup.parse(html);
    		jdoc.setBaseUri(url);
    		Elements links = jdoc.select("a[href]");
    		
    		for(Element link : links) {
    			String absolute = link.attr("abs:href");
    			String normalized = null;
    			if(url.startsWith("http://")) {
    				URLInfo info = new URLInfo(url);
    				normalized = normalize(url,  info, absolute);
    			}else {
    				normalized = normalizeHttps(url,  httpsUrl, absolute);
    			}			
    			collector.emit(new Values<Object>(normalized, doc, "false"));
    			XPathCrawler.getInstance().incrementInflightMessages();
    		} 
    	}
    	DocumentParserBolt.activeThreads.getAndDecrement();
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
