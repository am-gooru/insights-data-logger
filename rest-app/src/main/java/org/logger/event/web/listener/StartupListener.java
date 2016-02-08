/*******************************************************************************
 * StartupListener.java
 * insights-event-logger
 * Created by Gooru on 2014
 * Copyright (c) 2014 Gooru. All rights reserved.
 * http://www.goorulearning.org/
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/
package org.logger.event.web.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.logger.event.datasource.infra.Register;
import org.logger.event.datasource.infra.Registry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.context.ContextLoaderListener;


/**
 * <p>StartupListener class used to initialize and database settings
 * and populate any application-wide drop-downs.
 * 
 * <p>Keep in mind that this listener is executed outside of OpenSessionInViewFilter,
 * so if you're using Hibernate you'll have to explicitly initialize all loaded data at the 
 * Dao or service level to avoid LazyInitializationException. Hibernate.initialize() works
 * well for doing this.
 *
 */
public class StartupListener extends ContextLoaderListener implements ServletContextListener {
    
	private static final Logger logger = LoggerFactory.getLogger(StartupListener.class);

    public void contextInitialized(ServletContextEvent event) {
        
    	if (logger.isInfoEnabled()) {
    		logger.info("initializing context...");
        }
    	startApplication();
        // call Spring's context ContextLoaderListener to initialize
        // all the context files specified in web.xml
        super.contextInitialized(event);

        
        if (logger.isInfoEnabled()) {
        	logger.info("Event Logger API Context Initialization Complete [OK]");
        }
    
    }
    private void startApplication() {
        Registry registry = new Registry();
        try {
          for (Register register : registry) {
        	  	register.init();
          }
        } catch(IllegalStateException ie) {
        	logger.error("Error initializing application", ie);
        }
      }
}
