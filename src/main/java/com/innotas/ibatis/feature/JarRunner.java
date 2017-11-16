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

import java.io.IOException;
import java.net.URL;
import java.net.MalformedURLException;
import java.lang.reflect.InvocationTargetException;

/**
 * Runs a jar application from any url. Usage is 'java JarRunner url [args..]'
 * where url is the url of the jar file and args is optional arguments to be
 * passed to the application's main method.
 * 
 * Origin: http://docs.oracle.com/javase/tutorial/deployment/jar/jarrunner.html
 */
public class JarRunner {
	public static void main(String[] args) {
		if (args.length < 1) {
			usage();
		}
		URL url = null;
		try {
			url = new URL(args[0]);
		}
		catch (MalformedURLException e) {
			fatal("Invalid URL: " + args[0]);
		}
		// Create the class loader for the application jar file
		JarClassLoader cl = new JarClassLoader(url);
		// Get the application's main class name
		String name = null;
		try {
			name = cl.getMainClassName();
		}
		catch (IOException e) {
			System.err.println("I/O error while loading JAR file:");
			e.printStackTrace();
			System.exit(1);
		}
		if (name == null) {
			fatal("Specified jar file does not contain a 'Main-Class'" + " manifest attribute");
		}
		// Get arguments for the application
		String[] newArgs = new String[args.length - 1];
		System.arraycopy(args, 1, newArgs, 0, newArgs.length);
		// Invoke application's main class
		try {
			cl.invokeClass(name, newArgs);
		}
		catch (ClassNotFoundException e) {
			fatal("Class not found: " + name);
		}
		catch (NoSuchMethodException e) {
			fatal("Class does not define a 'main' method: " + name);
		}
		catch (InvocationTargetException e) {
			e.getTargetException().printStackTrace();
			System.exit(1);
		}
	}

	private static void fatal(String s) {
		System.err.println(s);
		System.exit(1);
	}

	private static void usage() {
		fatal("Usage: java JarRunner url [args..]");
	}
}