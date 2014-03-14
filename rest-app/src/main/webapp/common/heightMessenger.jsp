#-------------------------------------------------------------------------------
# heightMessenger.jsp
# insights-event-logger
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
<script>
	var heightMessengerUrl = '<%=request.getAttribute("heightMessenger.URL")%>';

	function sendUnloadMessage(forwardCall) {

		var message = "unload@0";

		var elem = document.getElementById("heightProxy");  
		elem.contentWindow.location.replace(heightMessengerUrl + "#" + message);
		
		var width = parseInt(elem.width);
	   	
		elem.width = width > 100 ? 100 : 200;	

		setTimeout(forwardCall,200);			

	}
	
	//Send the message to the parent for setting the height
	function sendHeightMessage(containerDiv) {
			
		var iframeHeight = document.getElementById(containerDiv).offsetHeight + 200;

		sendHeight(iframeHeight);		
	}

	function sendHeight(height) {
		
		var iframeHeight = height;
		var currentURL = self.location;
		
		var message = iframeHeight + "@" + currentURL;
		//var iframeHeight = document.body.offsetHeight + 100;
		
		var elem = document.getElementById("heightProxy");  

	   	elem.contentWindow.location.replace(heightMessengerUrl + "#" + message);
	   	
	   	var width = parseInt(elem.width);
	   	
		elem.width = width > 100 ? 100 : 200;
		
	}

	function sendTeachMessage(classPlanId){
		
		var currentURL = self.location;
		var message = "teach@" + classPlanId;

		var elem = document.getElementById("heightProxy");  

	   	elem.contentWindow.location.replace(heightMessengerUrl + "#" + message);

		var width = parseInt(elem.width);
	   	
		elem.width = width > 100 ? 100 : 200;
	}

	function sendSearchMessage() {
		
		var currentURL = "www.google.com";
		
		var message = "1300@" + currentURL;
		//var iframeHeight = document.body.offsetHeight + 100;
		
		var elem = document.getElementById("heightProxy");  
	   	elem.contentWindow.location.replace(heightMessengerUrl + "#" + message);
	   	
	   	var width = parseInt(elem.width);
	   	
		elem.width = width > 100 ? 100 : 200;		
	}

			
</script>
<iframe name = "heightProxy" id ="heightProxy" src = "<%=request.getAttribute("heightMessenger.URL")%>" style="position:absolute; top:-940px; left:-450px" width="200" height="100"> </iframe>
