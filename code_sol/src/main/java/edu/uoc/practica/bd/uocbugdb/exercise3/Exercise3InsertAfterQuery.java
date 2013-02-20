package edu.uoc.practica.bd.uocbugdb.exercise3;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import edu.uoc.practica.bd.util.DBAccessor;

public class Exercise3InsertAfterQuery {
	public static void main(String[] args) {
		Exercise3InsertAfterQuery app = new Exercise3InsertAfterQuery();
		app.run();
	}

	private void run() {
		DBAccessor dbaccessor = new DBAccessor();
		dbaccessor.init();
		Connection conn = dbaccessor.getConnection();
		// TODO Execute query
		if (conn != null) {
			Statement statement = null;
			PreparedStatement pstmtInsertInterview = null;
			ResultSet resultSet = null;
			
			try {
				statement = conn.createStatement();
				
				String sql = "SELECT w.dni_number, w.cif_code, w.init_date\n" + 
						"FROM worked w LEFT JOIN interview i ON (w.dni_number = i.dni_number AND w.cif_code = i.cif_code)\n" + 
						"WHERE end_date IS NULL and i.dni_number IS NULL";
				String sqlInsertInterview = "INSERT INTO interview(dni_number, cif_code, interview_date, accepted) " +
						"VALUES (?, ?, ?, TRUE);";
				pstmtInsertInterview = conn.prepareStatement(sqlInsertInterview);
				resultSet = statement.executeQuery(sql);

				// TODO Loop over over results and insert
				while (resultSet.next()) {
					String dniNumber 	= resultSet.getString(1);
					String cifCode		= resultSet.getString(2);
					Date initDate 		= resultSet.getDate(3);
					java.sql.Timestamp timestamp = new java.sql.Timestamp(initDate.getTime());
					pstmtInsertInterview.setString(1, dniNumber);
					pstmtInsertInterview.setString(2, cifCode);
					pstmtInsertInterview.setTimestamp(3, timestamp);
					pstmtInsertInterview.executeUpdate();
				}
				// TODO Validate transaction
				conn.commit();
				// TODO Close resources and check exceptions
			} catch (SQLException sqlException) {
				System.err.println("ERROR: Executing sql");
				System.err.println(sqlException.getMessage());
				try {
					conn.rollback();
				} catch (SQLException rollbackException) {
					System.err.println("ERROR: Executing rollback");
					System.err.println(rollbackException.getMessage());
				}
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
				if (pstmtInsertInterview != null) {
					try {
						pstmtInsertInterview.close();
					} catch (SQLException e) {
						System.err.println("ERROR: Closing pstmtCountClosed");
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
