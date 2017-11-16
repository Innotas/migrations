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



import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.ibatis.jdbc.RuntimeSqlException;

/**
 * ScriptRunner is in library mybatis, but we need it here. Initially my thought was to extend ScriptRunner, but that class has pretty much 
 * only private methods, so that option is not available.
 * 
 * So, instead of using the ScriptRunner from myBatis, we use our own that starts out as a copy of ScriptRunner.
 * 
 * New features we added:
 * <ul>
 * <li> Supports @RunJar to run a migration written in Java and packaged as a jar. This migration can run using multiple threads using multiple connections. </li>
 * <li> Ability to create Java stored procedures in Oracle; the statement needs to be wrapped (see OracleWrapper.sql)
 * </ul>
 * 
 * @author andrej
 */
public class InnotasScriptRunner {

  private static final String LINE_SEPARATOR = System.getProperty("line.separator", "\n");

  private static final String DEFAULT_DELIMITER = ";";

  private Connection connection;

  private boolean stopOnError;
  private boolean throwWarning;
  private boolean autoCommit;
  private boolean sendFullScript;
  private boolean removeCRs;
  private boolean escapeProcessing = true;

  private PrintWriter logWriter = new PrintWriter(System.out);
  private PrintWriter errorLogWriter = new PrintWriter(System.err);

  private String delimiter = DEFAULT_DELIMITER;
  private boolean fullLineDelimiter;
  
  private DataSource dataSource;
  
  /*
   * [Andrej] We allow migrations that use multiple threads and multiple connections; therefore, we need to a ConnectionProvider
   */
  public InnotasScriptRunner(Connection connection, DataSource dataSource) {
    this.connection = connection;
    this.dataSource = dataSource;
  }

  public void setStopOnError(boolean stopOnError) {
    this.stopOnError = stopOnError;
  }

  public void setThrowWarning(boolean throwWarning) {
    this.throwWarning = throwWarning;
  }

  public void setAutoCommit(boolean autoCommit) {
    this.autoCommit = autoCommit;
  }

  public void setSendFullScript(boolean sendFullScript) {
    this.sendFullScript = sendFullScript;
  }

  public void setRemoveCRs(boolean removeCRs) {
    this.removeCRs = removeCRs;
  }

  /**
   * @since 3.1.1
   */
  public void setEscapeProcessing(boolean escapeProcessing) {
    this.escapeProcessing = escapeProcessing;
  }

  public void setLogWriter(PrintWriter logWriter) {
    this.logWriter = logWriter;
  }

  public void setErrorLogWriter(PrintWriter errorLogWriter) {
    this.errorLogWriter = errorLogWriter;
  }

  public void setDelimiter(String delimiter) {
    this.delimiter = delimiter;
  }

  public void setFullLineDelimiter(boolean fullLineDelimiter) {
    this.fullLineDelimiter = fullLineDelimiter;
  }

  public void runScript(Reader reader) {
    setAutoCommit();

    try {
      if (sendFullScript) {
        executeFullScript(reader);
      } else {
        executeLineByLine(reader);
      }
    } finally {
      rollbackConnection();
    }
  }

  private void executeFullScript(Reader reader) {
    StringBuilder script = new StringBuilder();
    try {
      BufferedReader lineReader = new BufferedReader(reader);
      String line;
      while ((line = lineReader.readLine()) != null) {
        script.append(line);
        script.append(LINE_SEPARATOR);
      }
      String command = script.toString();
      println(command);
      executeStatement(command);
      commitConnection();
    } catch (Exception e) {
      String message = "Error executing: " + script + ".  Cause: " + e;
      printlnError(message);
      throw new RuntimeSqlException(message, e);
    }
  }

  private void executeLineByLine(Reader reader) {
    StringBuilder command = new StringBuilder();
    try {
      BufferedReader lineReader = new BufferedReader(reader);
      String line;
      while ((line = lineReader.readLine()) != null) {
        command = handleLine(command, line);
      }
      commitConnection();
      checkForMissingLineTerminator(command);
    } catch (Exception e) {
      if (e instanceof SQLWarning) {
          /*
           * [Andrej] debugging this... Getting mysteriouos warning "Warning: execution completed with warning" 
           * when running migration script 20170822170712_fix_current_asset_udf_values.sql. No idea what the problem
           * is with this statement. Runs fine in another migration script.
           */
          SQLWarning w = (SQLWarning) e;
          while (w != null) {
              w.printStackTrace(System.out);
              System.out.println("*** errorCode: " + w.getErrorCode());
              System.out.println("*** hasCause: " + w.getCause());
              w = w.getNextWarning();
              System.out.println("checking next warning... " + (w != null));
          }
      }
      else {
          e.printStackTrace(System.out);
      }
      
      String message = "Error executing: " + command + ".  Cause: " + e;
      printlnError(message);
      throw new RuntimeSqlException(message, e);
    }
  }

  public void closeConnection() {
    try {
      connection.close();
    } catch (Exception e) {
      // ignore
    }
  }

  private void setAutoCommit() {
    try {
      if (autoCommit != connection.getAutoCommit()) {
        connection.setAutoCommit(autoCommit);
      }
    } catch (Throwable t) {
      throw new RuntimeSqlException("Could not set AutoCommit to " + autoCommit + ". Cause: " + t, t);
    }
  }

  private void commitConnection() {
    try {
      if (!connection.getAutoCommit()) {
        connection.commit();
      }
    } catch (Throwable t) {
      throw new RuntimeSqlException("Could not commit transaction. Cause: " + t, t);
    }
  }

  private void rollbackConnection() {
    try {
      if (!connection.getAutoCommit()) {
        connection.rollback();
      }
    } catch (Throwable t) {
      // ignore
    }
  }

  private void checkForMissingLineTerminator(StringBuilder command) {
    if (command != null && command.toString().trim().length() > 0) {
      throw new RuntimeSqlException("Line missing end-of-line terminator (" + delimiter + ") => " + command);
    }
  }

  private StringBuilder handleLine(StringBuilder command, String line) throws SQLException, UnsupportedEncodingException {
    String trimmedLine = line.trim();
    if (lineIsComment(trimmedLine)) {
        final String cleanedString = trimmedLine.substring(2).trim().replaceFirst("//", "");
        if(cleanedString.toUpperCase().startsWith("@DELIMITER")) {
            delimiter = cleanedString.substring(11,12);
            return command;
        }
      println(trimmedLine);
    } else if (commandReadyToExecute(trimmedLine)) {
        String cs = command.toString();
        if (cs.contains("@RunJar")) {
            println(cs);
            executeJar(cs.toString());
        }
        else {
          command.append(line.substring(0, line.lastIndexOf(delimiter)));
          //command.append(LINE_SEPARATOR);
          println(command);
          executeStatement(command.toString());
        }
      command.setLength(0);
    } else if (trimmedLine.length() > 0) {
      command.append(line);
      command.append(LINE_SEPARATOR);
    }
    return command;
  }

  private boolean lineIsComment(String trimmedLine) {
    return trimmedLine.startsWith("//") || trimmedLine.startsWith("--");
  }

  private boolean commandReadyToExecute(String trimmedLine) {
    // issue #561 remove anything after the delimiter
    return !fullLineDelimiter && trimmedLine.contains(delimiter) || fullLineDelimiter && trimmedLine.equals(delimiter);
  }

  private void executeStatement(String command) throws SQLException {
    boolean hasResults = false;
    String sql = command;
    if (removeCRs) {
      sql = sql.replaceAll("\r\n", "\n");
    }
    
    StopWatch watch = new StopWatch();
    
    if (requiresOracleWrapperStatement(sql)) {
        /*
         * Creating Java stored procedures must be wrapped, otherwise they don't compile properly.
         */
        try {
            executeOracleWrapperStatement(sql);
        }
        catch (SQLException ex) {
            if (stopOnError)
                throw ex;
            else
                printlnError("Error executing: " + command + ". Cause: " + ex);
        }
    }
    else {
        Statement statement = connection.createStatement();
        statement.setEscapeProcessing(escapeProcessing);
        
        if (stopOnError) {
          hasResults = statement.execute(sql);
          if (throwWarning) {
            // In Oracle, CRATE PROCEDURE, FUNCTION, etc. returns warning
            // instead of throwing exception if there is compilation error.
            SQLWarning warning = statement.getWarnings();
            if (warning != null) {
                /*
                 * [Andrej] On Oracle, getting a strange warning when executing the "create table" statement in script
                 * 20170822170712_fix_current_asset_udf_values.sql. No idea why. Can't reproduce in SQL Developer.
                 */
                if (warning.getErrorCode() == 17110 && warning.getMessage().equals("Warning: execution completed with warning")) {
                    printlnError(warning);
                }
                else {
                  throw warning;
                }
            }
          }
        } else {
          try {
            hasResults = statement.execute(sql);
          } catch (SQLException e) {
            String message = "Error executing: " + command + ".  Cause: " + e;
            printlnError(message);
          }
        }
        printResults(statement, hasResults);
        try {
          statement.close();
        } catch (Exception e) {
          // Ignore to workaround a bug in some connection pools
        }
    }
    
    
    watch.stop();
    watch.printReport();

  }

  private void printResults(Statement statement, boolean hasResults) {
    try {
      if (hasResults) {
        ResultSet rs = statement.getResultSet();
        if (rs != null) {
          ResultSetMetaData md = rs.getMetaData();
          int cols = md.getColumnCount();
          for (int i = 0; i < cols; i++) {
            String name = md.getColumnLabel(i + 1);
            print(name + "\t");
          }
          println("");
          while (rs.next()) {
            for (int i = 0; i < cols; i++) {
              String value = rs.getString(i + 1);
              print(value + "\t");
            }
            println("");
          }
        }
      }
    } catch (SQLException e) {
      printlnError("Error printing results: " + e.getMessage());
    }
  }

  private void print(Object o) {
    if (logWriter != null) {
      logWriter.print(o);
      logWriter.flush();
    }
  }

  private void println(Object o) {
    if (logWriter != null) {
      logWriter.println(o);
      logWriter.flush();
    }
  }

  private void printlnError(Object o) {
    if (errorLogWriter != null) {
      errorLogWriter.println(o);
      errorLogWriter.flush();
    }
  }
  
  
  private void executeJar(String command) throws SQLException {
      if (!command.startsWith("@RunJar"))
          throw new RuntimeException("command does not start with '@RunJar'");
      
        URL url = null;
        try {
            File file = new File("./" + command.substring(7).trim());
            url = file.toURI().toURL();
        }
        catch (MalformedURLException e) {
            throw new RuntimeException("Invalid URL: " + command.substring(7).trim(), e);
        }
        
        // Create the class loader for the application jar file
        JarClassLoader cl = new JarClassLoader(url, getClass().getClassLoader());
        // Get the application's main class name
        String name = null;
        try {
            name = cl.getMainClassName();
        }
        catch (IOException e) {
            throw new RuntimeException("I/O error while loading JAR file:", e);
        }
        if (name == null) {
            throw new RuntimeException("Specified jar file does not contain a 'Main-Class'" + " manifest attribute");
        }

        try {
            Class<?> clazz;
            try {
                clazz = cl.loadClass(name);
            }
            catch (ClassNotFoundException ex) {
                throw new RuntimeException("Class not found :" + name, ex);
            }
            Method m = clazz.getMethod("migrate", java.sql.Connection.class, String.class, Properties.class, DataSource.class);
            boolean ac = connection.getAutoCommit();
            try {
                System.out.println("Running class " + name + " from " + url);
                StopWatch watch = new StopWatch();
                
                // for now, always pass an empty environment. None of our migration scripts make use of this feature, so I'm not
                // initializing the environment. Ideally, we should create the environment properties from the @RunJar line
                Properties environment = new Properties();
                
                m.invoke(null, connection, command, environment, dataSource);
                
                watch.stop();
                watch.printReport();
            }
            finally {
                try {
                    connection.setAutoCommit(ac);
                }
                catch (SQLException ignore) {}
            }
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException("Class does not define a 'migrate' method: " + name, e);
        }
        catch (InvocationTargetException e) {
            if (e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            }
            else if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            e.printStackTrace();
            throw new RuntimeException("Unexpected Error", e);
        }
        catch (IllegalAccessException e) {
            e.printStackTrace();
            throw new RuntimeException("Unexpected Error", e);
        }

  }  
  
  /**
   * In order to create Java stored procedures in Oracle, we'll need to wrap the statment.
   * @author andrej
   */
  private String getOracleWrapperStatement() {
      InputStream in = getClass().getResourceAsStream("OracleWrapper.sql");
      try {
          BufferedReader b = new BufferedReader(new InputStreamReader(in));
          StringWriter w = new StringWriter();
          PrintWriter pw = new PrintWriter(w);
          
          String line = null;
          while ((line = b.readLine()) != null) {
              pw.print(line);
              pw.print("\n");
          }
          pw.flush();
          
          return w.toString();
      }
      catch (IOException ex) {
          ex.printStackTrace();
          return null;
      }
  }
  
  private boolean requiresOracleWrapperStatement(String sql) {
      return sql.contains("java") &&
             sql.contains("source") &&
             sql.contains("create") &&
             sql.contains("class") &&
             sql.contains("compile");
  }
  
  /**
   * To compile "java source", we need to wrap the statement and use PL/SQL DBMS_SQL package.
   * Figured this out by reverse-engineering SQL Developer -- that's what it does.
   * (Andrej)
   */
  private void executeOracleWrapperStatement(String sql) throws SQLException {
      Clob clob = connection.createClob();
      clob.setString(1, sql);

      PreparedStatement ps = connection.prepareStatement(getOracleWrapperStatement());

      try {
          ps.setClob(1, clob);
          ps.executeUpdate();
      }
      finally {
          ps.close();
      }
  }

}

