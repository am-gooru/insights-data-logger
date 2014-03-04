#-------------------------------------------------------------------------------
# crocodocpdf.jsp
# insights-write-api
# Created by Gooru on 2014
# Copyright (c) 2014 Gooru. All rights reserved.
# http://www.goorulearning.org/
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#-------------------------------------------------------------------------------
<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Crocodoc Viewer</title>
</head>
<body>
<body style="margin:0;">
  <!--jQuery-->
  <script src="//ajax.googleapis.com/ajax/libs/jquery/1.6/jquery.min.js"></script>

  <!--docviewer.js-->
  <script src="//static-v2.crocodoc.com/core/docviewer.js"></script>

  <!--sets a global variable "_doc" that is needed for initialization-->
  <script src="//crocodoc.com/webservice/document.js?session=${session}"></script>

	<style type="text/css">
		    /*background*/
		    #DocViewer .doc { background:#424242; }
		
		    /*page borders*/
	    	#DocViewer .page { box-shadow:1px 1px 3px #999; outline:none; }
	    	#DocViewer .doc { background:#424242; overflow:scroll; } /*hide scrollbars*/
    		button.prev { position:absolute; top:50%; left:200px; margin-top:-15px; font-size:20px; }
    		button.next { position:absolute; top:50%; right:200px; margin-top:-15px; font-size:20px; }
    		
    		 #DocViewer { position:absolute; top:30px; bottom:0; left:0; right:0; }

		    .toolbar { background:#424242; height:29px; cursor:default; border-bottom:1px solid #aaa; }
		    .zoom-btns { position:absolute; left:4px; top:2px; width:150px; z-index:2; }
		    .page-nav { position:absolute; top:2px; width:100%; text-align:center; }
		
		    button { line-height:18px; }
		    .label { display:inline-block; width:80px; }
		    
	  </style>

<div class="toolbar">
    <!--zoom-->
    <div class="zoom-btns">
      <button class="zoom-out">-</button>
      <button class="zoom-in">+</button>
    </div>

    <!--page navigation-->
    <div class="page-nav">
      <button class="prev">◀</button>
      <span class="label">Page <span class="num">1</span>/<span class="numpages">1</span></span>
      <button class="next">▶</button>
    </div>
  </div>

  <%--  <!--div for viewer-->
	 <% var status =%>'${status}'';
 	<%if( status != "error"){ %>
  		<div id="DocViewer"></div>
  <% } else{%>
  		<div id="errorViewer">
  			<span>Sorry, we were unable to find the document at the original source. Verify that the document still exists</span>
  		</div>
  <%}%>
   --%>
  	<div id="DocViewer"></div>
  	
  <script type="text/javascript">
    //initialize docviewer
    var docViewer = new DocViewer({ "id": "DocViewer",zoom: "auto", page: 1 });

    //on docviewer ready
    docViewer.ready(function(e) {
        $('.numpages').text(e.numpages);
    });

    //toolbar events
    $('.zoom-in').click(function() {
        docViewer.zoom('in');
    });
    $('.zoom-out').click(function() {
        docViewer.zoom('out');
    });
    $('.prev').click(function() {
        docViewer.scrollTo('prev');
    });
    $('.next').click(function() {
        docViewer.scrollTo('next');
    });

    //docviewer events
    docViewer.bind('pagechange',function(e) {
        $('.num').text(e.page);
    });
    
  </script>
</body>
</body>
</html>
