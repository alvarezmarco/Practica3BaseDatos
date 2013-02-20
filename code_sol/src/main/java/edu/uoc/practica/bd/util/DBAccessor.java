package edu.uoc.practica.bd.util;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class DBAccessor {
	private String dbname;
	private String host;
	private String port;
	private String user;
	private String passwd;

	/**
	 * Initializes the class loading the database properties file and assigns
	 * values to the instance variables.
	 * 
	 * @throws RuntimeException
	 *             Properties file could not be found.
	 */
	public void init() {
		Properties prop = new Properties();
		InputStream propStream = this.getClass().getClassLoader()
				.getResourceAsStream("db.properties");
		try {
			prop.load(propStream);
			this.host = prop.getProperty("host");
			this.port = prop.getProperty("port");
			this.dbname = prop.getProperty("dbname");
			this.user = prop.getProperty("user");
			this.passwd = prop.getProperty("passwd");
		} catch (IOException e) {
			String message = "ERROR: db.properties file could not be found";
			System.err.println(message);
			throw new RuntimeException(message, e);
		}
	}

	/**
	 * Obtains a {@link Connection} to the database, based on the values of the
	 * <code>db.properties</code> file.
	 * 
	 * @return DB connection or null if a problem occurred when trying to
	 *         connect.
	 */
	public Connection getConnection() {
		Connection conn = null;

		// TODO Implement the DB connection
		String url = null;
		try {
			// Loads the driver
			Class.forName("org.postgresql.Driver");

			// Connects
			StringBuffer sbUrl = new StringBuffer();
			sbUrl.append("jdbc:postgresql:");
			if (host != null && !host.equals("")) {
				sbUrl.append("//").append(host);
				if (port != null && !port.equals("")) {
					sbUrl.append(":").append(port);
				}
			}
			sbUrl.append("/").append(dbname);
			url = sbUrl.toString();
			conn = DriverManager.getConnection(url, user, passwd);

			conn.setAutoCommit(false);
		} catch (ClassNotFoundException e1) {
			System.err.println("ERROR: Unable to load JDBC driver");
			System.err.println(e1.getMessage());
		} catch (SQLException e2) {
			System.err.println("ERROR: Unable to connect to the DB " + url);
			System.err.println(e2.getMessage());
		}

		// TODO Sets the search_path
		if (conn != null) {
			Statement statement = null;
			try {
				statement = conn.createStatement();
				statement.executeUpdate("SET search_path TO uoc_bugdb");
			} catch (SQLException e) {
				System.err.println("ERROR: Unable to set search_path");
				System.err.println(e.getMessage());
			} finally {
				try {
					statement.close();
				} catch (SQLException e) {
					System.err.println("ERROR: Closing statement");
					System.err.println(e.getMessage());
				}
			}
		}
		return conn;
	}
}
