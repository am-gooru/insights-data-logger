/*******************************************************************************
 * PipedInputStreamProcessor.java
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
package org.ednovo.data.handlers;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.ecyrd.speed4j.StopWatch;

public class PipedInputStreamProcessor extends BaseDataProcessor implements DataProcessor {

	@Override
	public void handleRow(Object row) throws Exception {
    	String line;
    	StopWatch sw = new StopWatch();
    	// FIXME: Where are we closing BufferedReader???
    	BufferedReader bi = new BufferedReader(new InputStreamReader(System.in));

    	long lines = 0;
    	try {
			while ((line = bi.readLine()) != null) {
				getNextRowHandler().processRow(line);
				lines++;
				if(lines % 1000 == 0) {
					sw.stop("1000 records processed", "processed " + lines + " lines so far");
					sw.start();
				}
			}
			LOG.info("processed " + lines + " lines");
		} catch (IOException e) {
			LOG.error("Something went wrong while processing input", e);
		}
    	sw.stop("loglines");
		LOG.info(sw.toString());
	}

}
