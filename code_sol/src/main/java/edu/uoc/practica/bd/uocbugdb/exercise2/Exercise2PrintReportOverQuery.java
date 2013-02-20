package edu.uoc.practica.bd.uocbugdb.exercise2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import edu.uoc.practica.bd.util.Column;
import edu.uoc.practica.bd.util.DBAccessor;
import edu.uoc.practica.bd.util.Report;

public class Exercise2PrintReportOverQuery {
	public static void main(String[] args) {
		Exercise2PrintReportOverQuery app = new Exercise2PrintReportOverQuery();
		app.run();
	}

	private void run() {
		DBAccessor dbaccessor = new DBAccessor();
		dbaccessor.init();
		Connection conn = dbaccessor.getConnection();
		if (conn != null) {
			Statement statement = null;
			ResultSet resultSet = null;
			try {
				statement = conn.createStatement();
				// TODO Execute query
				String sql = "SELECT c.cif_code, c.name, AVG(salary), COUNT(DISTINCT i.dni_number)\n" + 
						"FROM worked w JOIN company c ON (c.cif_code = w.cif_code) \n" + 
						"LEFT JOIN interview i ON (c.cif_code = i.cif_code)\n" + 
						"WHERE c.sector = 'IT' and creation_time < '1997/01/01'\n" + 
						"GROUP BY c.cif_code, c.name\n" + 
						"HAVING COUNT(DISTINCT w.dni_number) > 10";
				resultSet = statement.executeQuery(sql);
				List<Column> columns = Arrays.asList( //
						new Column("CIF", 9, "cifCode"), //
						new Column("Company Name", 20, "companyName"), //
						new Column("Average salary", 8, "avgSalary"), //
						new Column("Number of interviews", 6, "numInterviews")
						);
				Report report = new Report();
				report.setColumns(columns);
				List<Object> list = new ArrayList<Object>();
				
				// TODO Loop over results and get the main values
				while (resultSet.next()) {
					String cifCode 		= resultSet.getString(1);
					String companyName  = resultSet.getString(2);
					float avgSalary		= Math.round(resultSet.getFloat(3)*(float)100.0)/(float)100.0; //Only two decimals
					
					int numInterviews	= resultSet.getInt(4);
					Exercise2Row row = new Exercise2Row(cifCode, companyName, avgSalary, numInterviews);
					list.add(row);
				}
				// TODO End loop
				report.printReport(list);
				// TODO Close All resources
			} catch (SQLException e) {
				System.err.println("ERROR: Executing query");
				System.err.println(e.getMessage());
			} finally {
				if (resultSet != null) {
					try {
						resultSet.close();
					} catch (SQLException e) {
						System.err.println("ERROR: Closing resultSet");
						System.err.println(e.getMessage());
					}
				}
				if (statement != null) {
					try {
						statement.close();
					} catch (SQLException e) {
						System.err.println("ERROR: Closing statement");
						System.err.println(e.getMessage());
					}
				}
				if (conn != null) {
					try {
						conn.close();
					} catch (SQLException e) {
						System.err.println("ERROR: Closing connection");
						System.err.println(e.getMessage());
					}
				}
			}
		}
	}
}
