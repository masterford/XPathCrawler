package edu.upenn.cis455.crawler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.SocketException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.HttpsURLConnection;

import org.apache.log4j.Logger;

import edu.upenn.cis.stormlite.OutputFieldsDeclarer;
import edu.upenn.cis.stormlite.TopologyContext;
import edu.upenn.cis.stormlite.bolt.IRichBolt;
import edu.upenn.cis.stormlite.bolt.OutputCollector;
import edu.upenn.cis.stormlite.routers.IStreamRouter;
import edu.upenn.cis.stormlite.tuple.Fields;
import edu.upenn.cis.stormlite.tuple.Tuple;
import edu.upenn.cis.stormlite.tuple.Values;
import edu.upenn.cis455.crawler.info.RobotsTxtInfo;
import edu.upenn.cis455.crawler.info.URLInfo;
import edu.upenn.cis455.storage.DocVal;
import edu.upenn.cis455.storage.StorageServer;
import test.edu.upenn.cis.stormlite.WordCounter;

public class CrawlerBolt implements IRichBolt {
	static Logger log = Logger.getLogger(WordCounter.class);
	
	Fields schema = new Fields("url", "document", "toStore"); //TODO:
	 
    /**
     * To make it easier to debug: we have a unique ID for each
     * instance of the WordCounter, aka each "executor"
     */
    String executorId = UUID.randomUUID().toString();
    
    /**
     * This is where we send our output stream
     */
    private OutputCollector collector;
    
    /*Private Class variables to aid with crawling */
    private final String USER_AGENT_HEADER = "User-Agent: cis455crawler\r\n";
    private final String USER_AGENT = "cis455crawler";
    private final String CLRF = "\r\n";
    private final int HTTPS_PORT = 443; //default HTTPS port
    private InetAddress hostMonitor;
    private DatagramSocket s;
    private static AtomicInteger activeThreads;
    
    public CrawlerBolt() {
    	CrawlerBolt.activeThreads = new AtomicInteger();
    	hostMonitor = XPathCrawler.getInstance().getHostMonitor();
    	try {
			s = new DatagramSocket();
		} catch (SocketException e) {
			e.printStackTrace();
		}
    }
    
    public static int getActiveThreads() {
    	return CrawlerBolt.activeThreads.get();
    }
    
    /**
     * Initialization, just saves the output stream destination
     */
    @Override
    public void prepare(Map<String,String> stormConf, 
    		TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    private void parseHeader(String line, HashMap<String, String> headerMap) {
		String[] split = line.split(":", 2); //split once
		if(split.length == 2) {
			headerMap.put(split[0].toLowerCase(), split[1].trim()); //lowercase
		}
	}
  
  private String parseResponse(HashMap<String, String> responseHeaders, InputStream inputStream, int requestType) throws IOException {	   
		//InputStream inputStream = socket.getInputStream();
		int b;
		StringBuilder requestBuilder = new StringBuilder();
		Reader reader = new InputStreamReader(inputStream, StandardCharsets.US_ASCII);		
		String line = "tth"; 
		//while((b = inputStream.read()) != -1) {
		while((b = reader.read()) != -1) {
			char c = (char) b;
			requestBuilder.append(Character.toString(c));											
			if ( c == '\n') { //end of line	/home/cis455/git/HW2/berkeleydb
				line = requestBuilder.toString();
				if(line.startsWith("HTTP")) {
					String [] result = line.split(" ");
					responseHeaders.put("Code", result[1]);
				}				 
				 parseHeader(line, responseHeaders);
				 requestBuilder.delete(0, line.length()); //flush					 						 							  							
			}
			if(line.isBlank()) { //end of stream
				if(responseHeaders.get("content-length") != null && requestType == 1) { //body for get request	
					requestBuilder = new StringBuilder();
					 int size =  Integer.parseInt(responseHeaders.get("content-length").trim());
					 int count = 0;
					 while(count < size) {
						 b = reader.read();
						 requestBuilder.append((char) b);
						 count++;
					}
				 } 
				break;
			}
		}
		return requestBuilder.toString();
  }
  
  /*Checks Robots.txt to see if we can crawl given file path  */
  private boolean canCrawl(RobotsTxtInfo robotInfo, String filePath) {  
		 if(robotInfo.containsUserAgent(USER_AGENT)) { //only care about directives for this user agent
			 ArrayList<String> disallowedLinks = robotInfo.getDisallowedLinks(USER_AGENT);
			 for(String link : disallowedLinks) {
				 if(link.equals(filePath) || (link.startsWith(filePath) && !(filePath.equals("/") ))) {
					 return false;
				 }
			 }	
			// currentCrawlDelay = robotInfo.getCrawlDelay("*");
		 }else if(robotInfo.containsUserAgent("*")) { //else we care about this directive
			 ArrayList<String> disallowedLinks = robotInfo.getDisallowedLinks("*");
			 for(String link : disallowedLinks) {
				 if(link.equals(filePath) || (link.startsWith(filePath) && !(filePath.equals("/") ))) {
					 return false;
				 }
			 }			
		 }
		 return true; //default
  }
  
  /*Creates a normalized URL of the form protocol // hostname:portNo/filepath. e.g: http://crawltest.cis.upenn.edu:80/foo.html
   * @param oldURL : The current url/page where we found the new link.
   * @param URLInfo: URLInfo object for current URL
   * @param redirectURL: New URL to visit, could be either relative or absolute
   *  */
  private String normalize(String oldURL, URLInfo urlInfo, String redirectURL) {
	  StringBuilder newURL;
	  if(redirectURL.startsWith("http")) { //already absolute
		   newURL = new StringBuilder(redirectURL);
		   //append port number
		   String portString = ":" + Integer.toString(urlInfo.getPortNo());
		   URLInfo info = new URLInfo(redirectURL);
		  if(!redirectURL.contains(portString)){ //append port
			   //check if host names are equivalent
			   if(info != null && info.getHostName() != null && info.getHostName().equals(urlInfo.getHostName())) {
				   int index = redirectURL.startsWith("https://") ? 8 : 7;
				   index += urlInfo.getHostName().length();
				   newURL.insert(index, portString);
			   } else {
				   //append default port
				   String defaultPortString = ":80";
				   int index = 7 + info.getHostName().length();
				   newURL.insert(index, defaultPortString);
			   }
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
  
  private void sendRequest(int type, String filePath, String hostHeader, OutputStream out, Date lastModified) throws IOException {
	  /*Send HEAD/GET Request  */
	    String typeString = (type == 0) ? "HEAD " : "GET ";
		String headHeader = typeString + filePath + " HTTP/1.1\r\n";
		
		out.write(headHeader.getBytes());
	//	System.out.println(headHeader);
		out.write(hostHeader.getBytes());
		out.write(USER_AGENT_HEADER.getBytes());
		if(lastModified != null) {
			DateFormat df = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss zzz");
			df.setTimeZone(TimeZone.getTimeZone("GMT"));
			String modifiedSince = "If-Modified-Since: " + df.format(lastModified) + "\r\n";
			out.write(modifiedSince.getBytes());
		}
		out.write(CLRF.getBytes());		
		out.flush();	
  }
  
  private void sendMonitoring(String url) throws IOException {
	  byte[] data = ("ransford;" + url).getBytes();
	  DatagramPacket packet = new DatagramPacket(data, data.length, hostMonitor, 10455);
	  s.send(packet);
  }
  
  /*Checks whether the document is a valid html or xml  */
  private boolean isValidType(String type) {
	  return (type.contains("text/html") || type.contains("text/xml") || type.endsWith(".html") || type.endsWith(".xml") || type.equals("application/xml"));
  }
  private void parseRobotTxt(BufferedReader reader, String host) {
	  RobotsTxtInfo info = new RobotsTxtInfo();
	  String line;
		try {
			while((line = reader.readLine()) != null) {
				if(line.isBlank() || line.startsWith("#")) {
					continue;
				}
				String[] tuple = line.split(":");
				if(tuple.length != 2) {
					continue;
				}else {
					if (tuple[0].equals("User-agent")){								
						HashSet<String> agents = new HashSet<String>(); //set to hold all user agents for this directive
						agents.add(tuple[1].trim());
						info.addUserAgent(tuple[1].trim());
						String map = "";
						while((map = reader.readLine()) != null && !map.isBlank()) {
							String [] mapping = map.split(":");
							if(mapping.length != 2) {
								break;
							}
							if(mapping[0].equals("User-agent")) {
								agents.add(mapping[1].trim());
								info.addUserAgent(mapping[1].trim());
							}
							else if(mapping[0].equals("Disallow")) {
								for(String agent : agents) {
									info.addDisallowedLink(agent, mapping[1].trim());
								}										
							}else if(mapping[0].equals("Crawl-delay")) {
								for(String agent: agents) {
									info.addCrawlDelay(agent, Integer.parseInt(mapping[1].trim()));
								}									
							}else if(mapping[0].equals("Allow")) {
								for(String agent: agents) {
									info.addAllowedLink(agent, mapping[1].trim());
								}										
							}
						}
					}
				}
			}
		} catch (NumberFormatException | IOException e) {			
			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}			
			XPathCrawler.getInstance().getRobotMap().put(host, info);
		}
  }
  
    /**
     * Process a tuple received from the stream, tuple consists of a url which the crawler should crawl and outputs documents
     * 
     */
    @Override
    public void execute(Tuple input) {
    	 /*Basic Algorithm:
  	   *Intialize Q with a set of seed URLS
  	   *Pick the first URL from Q and download the corresponding page
  	   *Extract All URLS from the page
  	   *Append to Q any URLS that meet a) our criteria and b) are not already in P
  	   *Repeat whilst Q is not empty
  	   *  */
    	XPathCrawler.getInstance().decrementInflightMessages(); //a message has been routed to this bolt hence we decrement number of inflight messages
    	if(XPathCrawler.getInstance().getFileCount().get() >= XPathCrawler.getInstance().getmaxDocSize()) {
    		XPathCrawler.getInstance().shutdown();
    		System.out.println("Crawler Bolt Called Shutdown");
    		return;
    	}
        String url = input.getStringByField("url");
        if(url == null) { //TODO: Check shutdown?
        	return;
        }
        
        CrawlerBolt.activeThreads.getAndIncrement(); //increment number of active threads
        HashMap<String, RobotsTxtInfo> robotMap = XPathCrawler.getInstance().getRobotMap();
        HashMap<String, Date> lastCrawled = XPathCrawler.getInstance().getLastCrawled();
        
        if(url.startsWith("https://")) {
			  try {
					URL httpsUrl = new URL(url);					
					if(!robotMap.containsKey(httpsUrl.getHost())){ //get robots.txt
						StringBuilder robotUrl = new StringBuilder(url); //url for /robots.txt		
						String portString = ":" + Integer.toString(httpsUrl.getPort());
						int index;
						if(httpsUrl.getPort() != -1) {
							index = 8 + httpsUrl.getHost().length() + portString.length() + 1;	
						}else {
							index = 8 + httpsUrl.getHost().length() + 1;	
						}					
					    robotUrl.insert(index, "/robots.txt");
						URL robot = new URL(robotUrl.toString());
						HttpsURLConnection conn = (HttpsURLConnection) robot.openConnection();
						conn.setRequestMethod("GET");
						conn.setRequestProperty("User-Agent", USER_AGENT);
						//conn.get
						
						/*------------------------<*/
						sendMonitoring("/robots.txt");
						int responseCode = conn.getResponseCode();
						if(responseCode == HttpURLConnection.HTTP_OK) {						
							BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.US_ASCII));
							parseRobotTxt(reader, httpsUrl.getHost());						
						}					
						else {
							robotMap.put(robot.getHost(), new RobotsTxtInfo()); //no robots.txt found
						}
						conn.disconnect();
				}				
					/*-------------------->*/
					  
					  String filePath = httpsUrl.getPath();
					  String hostName = httpsUrl.getHost();
						/*Check if we can crawl file path */
						RobotsTxtInfo robotInfo = robotMap.get(hostName);
						if(!canCrawl(robotInfo, filePath)) {
							activeThreads.getAndDecrement();
							return; //skip //TODO: exit
						}
						
						/*Check crawl Delay  */
						if(robotInfo.crawlContainAgent(USER_AGENT) || (robotInfo.getCrawlDelay("*") != -1) ) {								
							if(lastCrawled.containsKey(hostName)) {
								int delay = robotInfo.crawlContainAgent(USER_AGENT) ? robotInfo.getCrawlDelay(USER_AGENT) : robotInfo.getCrawlDelay("*");
								if(delay != -1) {
									Date previous = lastCrawled.get(hostName);
									Date now = new Date();
									if(now.getTime() - previous.getTime() < (delay * 1000)) { //checks if the last crawled time was longer than the crawl delay
										//System.out.println(delay);
										XPathCrawler.getInstance().getFrontier().enqueue(url); //re add back to queue and don't crawl
										CrawlerBolt.activeThreads.getAndDecrement();
										return;
									}
								}else {
									lastCrawled.put(hostName, new Date());
								}	
							}							
						}
								
						/*Send Head Request  */		
						DocVal doc = XPathCrawler.getInstance().getDB().getDocInfo(url);
						HttpsURLConnection conn = (HttpsURLConnection) httpsUrl.openConnection();
						conn.setRequestMethod("HEAD");
						conn.addRequestProperty("User-Agent", USER_AGENT);
						if (doc != null) {							
							conn.setIfModifiedSince(doc.getLastChecked().getTime());
						}	
						sendMonitoring(url);
						/*Check Head Response  */
						int responseCode = conn.getResponseCode();
						String type = conn.getContentType();
						int statusXX = responseCode / 100;
						if(responseCode == 304 && doc != null) { //don't download document, retrieve cached document and extract links
							//retrieve document
							System.out.println(url + ": Not modified");							
							lastCrawled.put(hostName, new Date()); //update last crawled
							collector.emit(new Values<Object>(url, doc, "false")); //emit document but tell next bolt not to store it
							XPathCrawler.getInstance().incrementInflightMessages();
							CrawlerBolt.activeThreads.getAndDecrement();
							return;
						}
						
						if(statusXX == 3) { //check for redirects: 3xx URLS
							if(conn.getHeaderField("Location") != null) {
								String newUrl = normalizeHttps(url, httpsUrl, conn.getHeaderField("Location")); 					
								XPathCrawler.getInstance().getFrontier().enqueue(newUrl);
							}
							conn.disconnect();
							CrawlerBolt.activeThreads.getAndDecrement();
							return; //continue run
						}
						if(statusXX == 4 || statusXX == 5) {
							conn.disconnect();
							CrawlerBolt.activeThreads.getAndDecrement();
							return;
						}																	
						if(type == null || !isValidType(type)) {
							conn.disconnect();
							CrawlerBolt.activeThreads.getAndDecrement();
							return;
						}												
						BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.US_ASCII));																												
						int size = conn.getContentLength();
						if( size > XPathCrawler.getInstance().getmaxDocSize()) {
							conn.disconnect();
							CrawlerBolt.activeThreads.getAndDecrement();
							return;
						}						
						conn = (HttpsURLConnection) httpsUrl.openConnection();
						conn.setRequestMethod("GET");
						conn.addRequestProperty("User-Agent", USER_AGENT);
						sendMonitoring(url);
						lastCrawled.put(hostName, new Date()); //update last crawled
						responseCode = conn.getResponseCode();
						
						StringBuilder bodyBuilder = new StringBuilder();
						String line;
						reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.US_ASCII));
						
						while((line = reader.readLine()) != null) {
							bodyBuilder.append(line);
						}
						String body = bodyBuilder.toString();
						reader.close();
						
						/*Emit document */
						String contentType = conn.getContentType();
						DocVal store = new DocVal(size, contentType, body, new Date());
						System.out.println(url + ": Downloading");
						collector.emit(new Values<Object>(url, store, "true")); //emit document and tell next bolt to store doc	
						XPathCrawler.getInstance().incrementInflightMessages();
						conn.disconnect();	
						CrawlerBolt.activeThreads.getAndDecrement();
					  /*<---------------------*/				
					
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			  
		  } else {
			  URLInfo urlInfo = new URLInfo(url);
			  
			  String portString = ":" + Integer.toString(urlInfo.getPortNo());
			  if(!url.contains(portString)) {
				  url = normalize(null, urlInfo, url);
			  }
			  if(urlInfo.getHostName() == null) {
				  CrawlerBolt.activeThreads.getAndDecrement();
				  return; //skip this URL
			  }
			  try {
				Socket socket = new Socket(urlInfo.getHostName(), urlInfo.getPortNo());
				String hostHeader = "Host: " + urlInfo.getHostName() + CLRF;
										
				OutputStream out = socket.getOutputStream();
				
				/*Get and parse robots.txt for this host */
				if(!robotMap.containsKey(urlInfo.getHostName())) {
					
					sendRequest(1, "/robots.txt",  hostHeader, out, null); 
					sendMonitoring("/robots.txt");
					HashMap<String, String> responseHeaders = new HashMap<String, String>();
					String body = parseResponse(responseHeaders, socket.getInputStream(), 1);
					int responseCode = Integer.parseInt(responseHeaders.get("Code"));
					
					if((responseCode / 100 == 2) && body != null) { //2xx response code only
						String robotsTxt = new String(body);				
						BufferedReader reader = new BufferedReader(new StringReader(robotsTxt));					
						parseRobotTxt(reader, urlInfo.getHostName());				
					}else {
						robotMap.put(urlInfo.getHostName(), new RobotsTxtInfo()); //no robots.txt found
					}
				}
				
				String filePath = urlInfo.getFilePath();
				/*Check if we can crawl file path */
				RobotsTxtInfo robotInfo = robotMap.get(urlInfo.getHostName());
				if(!canCrawl(robotInfo, filePath)) {
					socket.close();
					CrawlerBolt.activeThreads.getAndDecrement();
					return; //skip
				}
				String hostName = urlInfo.getHostName();
				/*Check crawl Delay  */
				if(robotInfo.crawlContainAgent(USER_AGENT) || (robotInfo.getCrawlDelay("*") != -1) ) {								
					if(lastCrawled.containsKey(hostName)) {
						int delay = robotInfo.crawlContainAgent(USER_AGENT) ? robotInfo.getCrawlDelay(USER_AGENT) : robotInfo.getCrawlDelay("*");
						Date previous = lastCrawled.get(hostName);
						Date now = new Date();
						if(now.getTime() - previous.getTime() < (delay * 1000)) { //checks if the last crawled time was longer than the crawl delay
							XPathCrawler.getInstance().getFrontier().enqueue(url); //re add back to queue and don't crawl
							socket.close();
							CrawlerBolt.activeThreads.getAndDecrement();
							return;
						}
					}else {
						lastCrawled.put(hostName, new Date());
					}			
				}
						
				/*Send Head Request  */		
				DocVal doc = StorageServer.getInstance().getDocInfo(url);
				
				if (doc != null) {				
					sendRequest(0, urlInfo.getFilePath(),  hostHeader, out, doc.getLastChecked()); //doc already exists, check if unmodified
				}else {
					sendRequest(0, urlInfo.getFilePath(),  hostHeader, out, null); 
				}	
				sendMonitoring(url);
				/*Check Head Response  */
				HashMap<String, String> responseHeaders = new HashMap<String, String>();
				String body = parseResponse(responseHeaders, socket.getInputStream(), 0);
				int responseCode = Integer.parseInt(responseHeaders.get("Code"));
				String type = responseHeaders.get("content-type");
				
				if(responseCode == 304 && doc != null) { //don't download document, retrieve cached document and extract links
					//retrieve document
					System.out.println(url + ": Not modified");					
					collector.emit(new Values<Object>(url, doc, "false")); //don't store
					XPathCrawler.getInstance().incrementInflightMessages();
					lastCrawled.put(hostName, new Date()); //update last crawled
					socket.close();
					CrawlerBolt.activeThreads.getAndDecrement();
					return;
				}
				int statusXX = responseCode / 100;
				if(statusXX == 3) { //check for redirects: 3xx URLS
					if(responseHeaders.get("location") != null) {
						String newUrl = normalize(url, urlInfo, responseHeaders.get("location")); 					
						XPathCrawler.getInstance().getFrontier().enqueue(newUrl);
					}
					socket.close();
					CrawlerBolt.activeThreads.getAndDecrement();
					return; //continue run
				}
				if(statusXX == 4 || statusXX == 5) {
					socket.close();
					CrawlerBolt.activeThreads.getAndDecrement();
					return;
				}
				
				if(responseHeaders.get("content-length") == null){
					socket.close();
					CrawlerBolt.activeThreads.getAndDecrement();
					return;
				}
				
				if(type == null ||  !isValidType(type) ) {
					socket.close();
					CrawlerBolt.activeThreads.getAndDecrement();
					return;
				}
				
				int size = Integer.parseInt(responseHeaders.get("content-length"));
				if( size > XPathCrawler.getInstance().getmaxDocSize()) {
					socket.close();
					CrawlerBolt.activeThreads.getAndDecrement();
					return;
				}
				
				sendRequest(1, urlInfo.getFilePath(),  hostHeader, out, null); //Get request
				sendMonitoring(url);
				lastCrawled.put(hostName, new Date()); //update last crawled
				
				responseHeaders = new HashMap<String, String>();
				body = parseResponse(responseHeaders, socket.getInputStream(), 1);
				responseCode = Integer.parseInt(responseHeaders.get("Code"));
				
				/*Emit file with flag to store in DB */
				String contentType = responseHeaders.get("content-type");
				DocVal store = new DocVal(size, contentType, body, new Date());
				System.out.println(url + ": Downloading");
				collector.emit(new Values<Object>(url, store, "true"));
				XPathCrawler.getInstance().incrementInflightMessages();
				
				socket.close();	
				CrawlerBolt.activeThreads.getAndDecrement();
			} catch (IOException e) {			
				e.printStackTrace();	
				CrawlerBolt.activeThreads.getAndDecrement();
				return;
			}			  
		  }
	  	}		 
      //  collector.emit(new Values<Object>(word, String.valueOf(count)));
    

    /**
     * Shutdown, just frees memory
     */
    @Override
    public void cleanup() {
    	//System.out.println("WordCount executor " + getExecutorId() + " has words: " + wordCounter.keySet());
    	if(!s.isClosed()) { //close datagram socket
    		s.close();
    	}  	
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
