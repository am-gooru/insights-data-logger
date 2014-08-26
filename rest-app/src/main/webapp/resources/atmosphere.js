$(function () {
    "use strict";
    var header = $('#header');
    var content = $('#content');
    var input = $('#input');
    var socket = $.atmosphere;

    // We are now ready to cut the request
    var request = { url: 'http://dev-insights.goorulearning.org:8080/insights-api-dev/atmosphere/push/message',
        contentType : "application/json",
        logLevel : 'debug',
        transport : 'websocket' ,
        fallbackTransport: 'long-polling'};

    request.onOpen = function(response) {
        content.html($('<p>', { text: 'Atmosphere connected using ' + response.transport }));
        input.removeAttr('disabled').focus();
    };

    request.onTransportFailure = function(errorMsg, request) {
        jQuery.atmosphere.info(errorMsg);
        if ( window.EventSource ) {
            request.fallbackTransport = "sse";
        }
        header.html($('<h3>', { text: 'Atmosphere Chat. Default transport is WebSocket, fallback is ' + request.fallbackTransport }));
    };

    request.onReconnect = function (request, response) {
        socket.info("Reconnecting")
    };

    request.onMessage = function (response) {
    	
        var message = response.responseBody;
        
        addMessage(message);
    };

    request.onClose = function(response) {
        logged = false;
    }

    request.onError = function(response) {
        content.html($('<p>', { text: 'Sorry, but there\'s some problem with your '
            + 'socket or the server is down' }));
    };

    var subSocket = socket.subscribe(request);
    
    function addMessage(json) {
        content.html('<p><span style="color:black">' + json + '</span> </p>');
    }
});
