/*******************************************************************************
 * LoaderConstants.java
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
package org.logger.event.cassandra.loader;

public enum LoaderConstants {
	
    CORRECT("correct"),
    
    INCORRECT("in-correct"),
    
    VIEWS("resource-view"),

	SKIPPED("skipped"),
 
	COMPLETED("completed"),
    
    INPROGRESS("in-progress"),
    
    REST_API_END_POINT("http://www.gooruleaning.org/gooruapi"),
    
    SESSIONTOKEN("session_token"),
    
    SEARCHINDEXAPI("search_indexing_api"),
    
    VIEW_COUNT_REST_API_END_POINT("view.count.update.rest.api"),
    
    CROPD("collection-resource-oe-play-dots"),
    
    QOPD("question-oe-play-dots"),
  
    CRPD("collection-resource-play-dots"),
    
    CQPD("collection-question-play-dots"), 
  
    CQRPD("collection-question-resource-play-dots"),
    
    RPD("resource-play-dots"),
    
    CP("collection-play"),
    
    CPD("collection-play-dots"),
    
    CRP("collection-resource-play"),
    
    QUIZPRV("quiz-preview"),
    
    RPRV("resource-preview"),
    
    QUIZP("quiz-play"),
    
    QPD("question-play-dots"),
    
    CR("create-reaction"),
    
    CSRCH("collection-search"),
    
    RSRCH("resource-search"),
    
    QSRCH("quiz-search"),
    
    SCSRCH("scollection-search"),
    
    RADD("resource-add"),
    
    SCIADD("scollection-item-add"),
    
    SCCOPY("scollection-copy"),
    
    CCOPY("collection-copy"),
    
    ESKL("kafka-logged"),
    
    ESAPIL("api-logged"),
    
    ESFL("file-logged"),
    
    KEY("key"),
    
    STATUS("status"),
    
    STARTTIME("start_time"),
    
    ENDTIME("end_time"),
    
    LASTPROCESSED("last_processed"),
    
    SCORE("score"),
    
    TS("time_spent"),
    
    COUNT("count"),
    
    AVGTS("avg_time_spent"),
    
    TOTALVIEWS("views"),
    
    AVGRA("avg_reaction"),
    
    CRPV1("collection.resource.play"),
    
    CPV1("collection.play"),
    
    RP1("resource.play"),
    
    CRAV1("reaction.create"),
    
	CLPV1("classpage.view"),
    
	CLPCV1("classpage.create"),
	
	CLUAV1("classpage.user.add"),
	
	CCV1("collection.create"),
	
	RUFB("resource.user.feedback"),
	
	CLASSPAGE("classpage"),
	
	ITEM_DOT_CREATE("item.create"),
	
	ITEM_DOT_EDIT("item.edit"),
	
	ITEM_DOT_DELETE("item.delete"),
	
	REGISTER_DOT_USER("user.register"),
	
	PROFILE_DOT_ACTION("profile.action"),
	
	CLP_USER_REMOVE("classpage.user.remove"),
	
	CLASSPAGE_DOT_COLLECTION("classpage.collection"),
	
	SHELF_DOT_COLLECTION("shelf.collection"),
	
	COLLECTION_DOT_RESOURCE("collection.resource"),
	
	SCOLLECTION("scollection"),
	
	PATHWAY("pathway"),
    
    ;
	
	String name;

	
	private LoaderConstants(String name) {
		this.name = name;
	}


	public String getName(){
		return name;
	}

}
