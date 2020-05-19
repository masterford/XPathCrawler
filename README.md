# XPathCrawler
This code was submitted for CIS555 at the University of Pennsylvania, assignment 2.

This application is a very versatile topic specific web crawler + XPathEngine + a frontend that allows users to create topic-specific "channels" defined by a set of XPath
expressions, and to display documents that match a channel;; one use of it could be to write an RSS aggregator with keyword-defined channels. In this case, the XPath expressions would find RSS feeds (which are just XML documents) that contain articles with the specified keywords, and the stylesheet would select the matching articles from the feeds and format them for the user. 

It involves:
• A crawler that traverses the Web, looking for HTML and XML documents that match one
of the XPath expressions;
• Routing documents from the crawler through a stream engine for processing one at a time;
• Writing an XPath evaluation engine that determines if an HTML or XML document matches one
of a set of XPath expressions; and
• Apersistent data store, using Oracle Berkeley DB, to hold retrieved HTML/XML
documents and channel definitions.