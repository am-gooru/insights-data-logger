#-------------------------------------------------------------------------------
# scripts.jsp
# rest-app
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
<%@ include file="/common/taglibs.jsp"%>+
<%@page import="org.springframework.security.core.Authentication"%>
<%@page import="org.springframework.security.core.context.SecurityContextHolder"%>
<%@page import="java.util.List"%>
<%@page import="org.springframework.security.core.GrantedAuthority"%>
<%@page import="java.util.Arrays"%>
<%@page import="java.util.ArrayList"%>

<script type="text/javascript" src="scripts/jquery/jquery-1.4.4.js"></script>
<script type="text/javascript" src="scripts/libraries/sammy/minified/sammy.min.js"  charset="utf-8"></script>
<script type="text/javascript" src="scripts/jquery/jquery.class.js"></script>
<script type="text/javascript" src="scripts/jquery/swfobject.js"></script>
<script type="text/javascript" src="scripts/core/core.min.js" ></script>
<script>
	var USER = {
		    id : '<c:out value="${USER.gooruUId}"/>',
		    emailId : '<c:out value="${USER.gooruUId}"/>',	    
		    role: '<c:out value="${USER.userRole.name}"/>',
		    firstName: '<c:out value="${USER.firstName}"/>',
		    lastName: '<c:out value="${USER.lastName}"/>',
		    token: '<c:out value="${SESSION_TOKEN}"/>'
	};
	var PAGE_STATUS = "NA";
		$.ajaxSetup({
		 data: {
		 	sessionToken: USER.token
		 }
 	});
</script>
<%!
	boolean isAnonymous() {
		Authentication currentAuthObj = SecurityContextHolder.getContext().getAuthentication();
	    List<GrantedAuthority> authorities = new ArrayList(currentAuthObj.getAuthorities());
	    for (GrantedAuthority auth : authorities) {
	    	if (auth.getAuthority().equals("ROLE_ANONYMOUS")) {
	        	return true;
	        }
	    }
	    return false;
	}
%>
