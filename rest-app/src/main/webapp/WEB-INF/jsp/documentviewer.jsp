<%@ include file="/common/taglibs.jsp"%>
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>Gooru Document Viewer</title>
<style type="text/css" media="screen"> 
			html, body	{ height:100%; }
			body { margin:0; padding:0; overflow:auto; text-align:center;}   
			#flashContent { display:none; }
</style>
</head>
<body>
	
  
	<div id="cb"> </div> 
	<script type="text/javascript" src="../flexpaper/js/swfobject/swfobject.js"></script> 
	<script type="text/javascript">
	 window.onload = function()
	{
	  var params = {
	    SwfFile : "${swfFile}",
	    Scale : 1
	  }
	  
	  swfobject.embedSWF("../flexpaper/FlexPaperViewer.swf","cb","100%","100%","9.0.0","../flexpaper/js/swfobject/expressInstall.swf", params);
	}	
	</script> 
	
</body>
</html>
