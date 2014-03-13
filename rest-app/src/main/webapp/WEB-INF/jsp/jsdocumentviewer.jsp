<%@ include file="/common/taglibs.jsp"%>
<html>
<head>
	<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
	<title>Gooru Document Viewer</title>
	<!-- In production, only one script (pdf.js) is necessary -->
	<!-- In production, change the content of PDFJS.workerSrc below -->
	<script type="text/javascript" src="../../pdfdotjs/lib/core.js"></script>
	<script type="text/javascript" src="../../pdfdotjs/lib/util.js"></script>
	<script type="text/javascript" src="../../pdfdotjs/lib/api.js"></script>
	<script type="text/javascript" src="../../pdfdotjs/lib/canvas.js"></script>
	<script type="text/javascript" src="../../pdfdotjs/lib/obj.js"></script>
	<script type="text/javascript" src="../../pdfdotjs/lib/function.js"></script>
	<script type="text/javascript" src="../../pdfdotjs/lib/charsets.js"></script>
	<script type="text/javascript" src="../../pdfdotjs/lib/cidmaps.js"></script>
	<script type="text/javascript" src="../../pdfdotjs/lib/colorspace.js"></script>
	<script type="text/javascript" src="../../pdfdotjs/lib/crypto.js"></script>
	<script type="text/javascript" src="../../pdfdotjs/lib/evaluator.js"></script>
	<script type="text/javascript" src="../../pdfdotjs/lib/fonts.js"></script>
	<script type="text/javascript" src="../../pdfdotjs/lib/glyphlist.js"></script>
	<script type="text/javascript" src="../../pdfdotjs/lib/image.js"></script>
	<script type="text/javascript" src="../../pdfdotjs/lib/metrics.js"></script>
	<script type="text/javascript" src="../../pdfdotjs/lib/parser.js"></script>
	<script type="text/javascript" src="../../pdfdotjs/lib/pattern.js"></script>
	<script type="text/javascript" src="../../pdfdotjs/lib/stream.js"></script>
	<script type="text/javascript" src="../../pdfdotjs/lib/worker.js"></script>
	<script type="text/javascript" src="../../pdfdotjs/external/jpgjs/jpg.js"></script>
	<script type="text/javascript" src="../../pdfdotjs/lib/jpx.js"></script>
	<script type="text/javascript" src="../../pdfdotjs/lib/jbig2.js"></script>
	
	<script type="text/javascript">
	  // Specify the main script used to create a new PDF.JS web worker.
	  // In production, change this to point to the combined `pdf.js` file.
	  PDFJS.workerSrc = '../../pdfdotjs/lib/worker_loader.js';
	
	  // Specify the PDF with AcroForm here
	  var pdfWithFormsPath = '${pdfFile}';
	</script>
	
	<style>
	.pdfpage { position:relative; top: 0; left: 0; border: solid 1px black; margin: 10px; }
	.pdfpage > canvas { position: absolute; top: 0; left: 0; }
	.pdfpage > div { position: absolute; top: 0; left: 0; }
	.inputControl { background: transparent; border: 0px none; position: absolute; margin: auto; }
	.inputControl[type='checkbox'] { margin: 0px; }
	.inputHint { opacity: 0.2; background: #ccc; position: absolute; }
	</style>
	
	<script type="text/javascript" src="../../pdfdotjs/forms.js"></script>
</head>

<body>
	<div id="viewer"></div>
</body>
</html>
