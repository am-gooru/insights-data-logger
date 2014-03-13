/*******************************************************************************
 * Copyright 2014 Ednovo d/b/a Gooru. All rights reserved.
 *  http://www.goorulearning.org/
 *  
 *  FileInputProcessor.java
 *  event-api-stable-1.1
 *  
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *   "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 * 
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 * 
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/
package org.ednovo.data.handlers;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;

import com.ecyrd.speed4j.StopWatch;

public class FileInputProcessor extends BaseDataProcessor implements DataProcessor {

	private final Map<String, String> fileInputData;

	public FileInputProcessor(Map<String, String> configOptionsMap) {
		this.fileInputData = configOptionsMap;
	}
	
	@Override
	public void handleRow(Object row) throws Exception {
		File folder = new File(fileInputData.get("file-path"));
		Collection<File> files = FileUtils.listFiles(folder, new WildcardFileFilter(fileInputData.get("path-pattern")), DirectoryFileFilter.DIRECTORY);
		StopWatch sw = new StopWatch();
		for (final File file : files) {
			LOG.info("processing file {}" , file.getAbsolutePath());
			sw.start();
			long lines = 0;
			try {
				LineIterator it = FileUtils.lineIterator(file, "UTF-8");
				 try {
				   while (it.hasNext()) {
				     final String line = it.nextLine();
				     
				     // Send the row to the next process handler.
				     getNextRowHandler().processRow(line);
				     
					 lines++;
					if(lines % 1000 == 0) {
						LOG.info("file-lines: {} ", lines);
					}
				   }
				 } finally {
				   LineIterator.closeQuietly(it);
				 }
			} catch (IOException e) {
				LOG.error("Error processing file {} " , file.getAbsolutePath(), e);
			}
	    	sw.stop("file:"+file.getAbsolutePath() + ": lines= "+lines + " ");
			LOG.info(sw.toString(Integer.parseInt(lines+"")));
		}
	}

}
