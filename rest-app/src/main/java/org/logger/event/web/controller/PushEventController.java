package org.logger.event.web.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import javax.servlet.http.HttpSession;

import org.atmosphere.cpr.AtmosphereRequest;
import org.atmosphere.cpr.AtmosphereResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;


@Controller
@RequestMapping(value="v2/")
public class PushEventController extends AtmosphereResolver {
	
	protected final Logger logger = LoggerFactory .getLogger(PushEventController.class);
	
	ArrayList<HttpSession> subscribers = new ArrayList<HttpSession>();
	
	@RequestMapping(value = "/push", method = RequestMethod.GET)
	public ModelAndView trackEvent(){
		return new ModelAndView("show");
	}
	

    @RequestMapping(value = "/push/message", method = RequestMethod.GET)
    @ResponseBody public void onRequest(AtmosphereResource atmosphereResource, HttpSession session) throws IOException {

        AtmosphereRequest atmosphereRequest = atmosphereResource.getRequest();
        
        if(atmosphereRequest.getHeader("negotiating") == null) {
            atmosphereResource.resumeOnBroadcast(atmosphereResource.transport() == AtmosphereResource.TRANSPORT.LONG_POLLING).suspend();
        } else {
            atmosphereResource.getResponse().getWriter().write("OK");
        }
        for(HttpSession httpSession : subscribers) {
        	subscribers.add(session);
        }

    }

    @RequestMapping(value = "/push/message", method = RequestMethod.POST)
    @ResponseBody public void onPost(AtmosphereResource atmosphereResource) throws IOException{

        AtmosphereRequest atmosphereRequest = atmosphereResource.getRequest();

       String data =  atmosphereRequest.getParameter("data");

        
        atmosphereResource.getBroadcaster().broadcast(data);
       
    }
    

}
