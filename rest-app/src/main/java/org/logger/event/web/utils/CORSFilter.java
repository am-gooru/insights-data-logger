/*******************************************************************************
 * CORSFilter.java
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
package org.logger.event.web.utils;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class CORSFilter implements Filter {
	
	public static String VALID_METHODS = "DELETE, HEAD, GET, OPTIONS, POST, PUT";

	public CORSFilter() { 
	}
	
	@Override
	public void destroy() {
	}

	@Override
	/*public void doFilter(ServletRequest request, ServletResponse response,
			FilterChain chain) throws IOException, ServletException {
		((HttpServletResponse) response).addHeader(
				"Access-Control-Allow-Origin", "*");
		chain.doFilter(request, response);

	}*/
	
	public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) throws ServletException, IOException {
        HttpServletRequest httpReq = (HttpServletRequest) req;
        HttpServletResponse httpResp = (HttpServletResponse) resp;

        // No Origin header present means this is not a cross-domain request
        String origin = httpReq.getHeader("Origin");
         if (origin == null) {
            // Return standard response if OPTIONS request w/o Origin header
           if ("OPTIONS".equalsIgnoreCase(httpReq.getMethod())) {
                httpResp.setHeader("Allow", VALID_METHODS);
                httpResp.setStatus(200);
                return;
            }
        } else {
            // This is a cross-domain request, add headers allowing access
            httpResp.setHeader("Access-Control-Allow-Origin", origin);
            httpResp.setHeader("Access-Control-Allow-Methods", VALID_METHODS);

            String headers = httpReq.getHeader("Access-Control-Request-Headers");
            if (headers != null)
                httpResp.setHeader("Access-Control-Allow-Headers", headers);

            // Allow caching cross-domain permission
            httpResp.setHeader("Access-Control-Max-Age", "3600");
        }
        // Pass request down the chain, except for OPTIONS
        if (!"OPTIONS".equalsIgnoreCase(httpReq.getMethod())) {
            chain.doFilter(req, resp);
        }
 }

	@Override
	public void init(FilterConfig filterConfig) throws ServletException {
	}

}
