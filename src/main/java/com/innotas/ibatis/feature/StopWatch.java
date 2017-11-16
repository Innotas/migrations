/**
 *    Copyright 2010-2017 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package com.innotas.ibatis.feature;

import java.util.Date;

public class StopWatch {
	long startTime = System.currentTimeMillis();
	long stopTime = -1 ;
	
	public void start() {
		startTime = System.currentTimeMillis();
		stopTime = -1;
	}
	
	public void stop() {
		stopTime = System.currentTimeMillis();
	}
	
	public String toString() {
		long hours = 0;
		long minutes = 0;
		long seconds = 0;
		
		long x = stopTime > 0L ? stopTime : System.currentTimeMillis();
		
		seconds = (x - startTime) / 1000L;
		if (seconds >= 60) {
			minutes = seconds / 60L;
			seconds = seconds % 60L;
		}
		
		if (minutes >= 60) {
			hours = minutes / 60L;
			minutes = minutes % 60L;
		}
		
		return String.format("Execution time: %dh %dm %ds", hours, minutes, seconds);
	}
	
	public void printReport() {
		System.out.println();
		System.out.println("|| Execution Statistics: ");
		System.out.println("|| Start: " + new Date(startTime));
		System.out.println("|| End: " + new Date(stopTime > 0 ? stopTime : System.currentTimeMillis()));
		System.out.println("|| " + toString());
		System.out.println("|| ---------------------------------------------------------------------");
		System.out.println();
	}

}
