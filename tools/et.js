(function() {
  if(typeof(_et.endPoint) == 'undefined' ||  _et.endPoint == '') {
      _et.endPoint =  "logapi.goorulearning.org";
  }
 var apiEndpoint = ('https:' == document.location.protocol ? 'https://' : 'http://') + _et.endPoint +"/api/log";


  var el = {
    authenticate: function() {
      var request = el.request();
      request.open('POST',apiEndpoint + "/event/authenticate?apiKey=" + _et.apiKey , true);
      request.setRequestHeader("Content-type","application/x-www-form-urlencoded");
      request.onreadystatechange=function() {
        if(request.readyState == 4) {
          var responseType = request.getAllResponseHeaders();
            if(responseType.indexOf("application/json") != -1) {
                var json = eval("(" + request.responseText + ")");
                if (request.status == 403) {
                    if (window.console) {
                        console.error("ET: Not able to authenticate");
                    }
                } else {
                    if (window.console) {
                        console.debug("ET: Authentication successful");
                    }
                }
            }
        }
      }
     request.send();
    },

    triggerCall: function() {
      if (_et.data.length > 0) {
            var request = el.request();
            request.open('POST',apiEndpoint + "/event?apiKey=" + _et.apiKey, false);
            request.setRequestHeader("Content-type","application/json");
            request.onreadystatechange=function() {
                if(request.readyState == 4) {
                var responseType = request.getAllResponseHeaders();
                    if(responseType.indexOf("application/json") != -1) {
                    var json = eval("(" + request.responseText + ")");
                    }
                }
            }
            request.send("[" + _et.data[0].toString() + "]");
            _et.data.splice(0, 1);
       }
    },

    request : function() {
      var request;
      if (window.XMLHttpRequest) {// code for IE7+, Firefox, Chrome, Opera, Safari
        request=new XMLHttpRequest();
      }
      else {// code for IE6, IE5
        request=new ActiveXObject("Microsoft.XMLHTTP");
      }
      return request;
    }
  };
  el.authenticate();
 _et.triggerCall = el.triggerCall;
})();

