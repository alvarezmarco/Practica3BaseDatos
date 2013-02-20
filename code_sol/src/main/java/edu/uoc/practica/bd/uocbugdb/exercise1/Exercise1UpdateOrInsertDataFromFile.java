package edu.uoc.practica.bd.uocbugdb.exercise1;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import edu.uoc.practica.bd.util.DBAccessor;
import edu.uoc.practica.bd.util.FileUtilities;

public class Exercise1UpdateOrInsertDataFromFile {
	private FileUtilities fileUtilities;

	public Exercise1UpdateOrInsertDataFromFile() {
		super();
		fileUtilities = new FileUtilities();
	}

	public static void main(String[] args) {
		Exercise1UpdateOrInsertDataFromFile app = new Exercise1UpdateOrInsertDataFromFile();
		app.run();
	}

	private void run() {
		List<List<String>> fileContents = null;
		try {
			fileContents = fileUtilities
					.readFileFromClasspath("exercise1.data");
		} catch (FileNotFoundException e) {
			System.err.println("ERROR: File not found");
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("ERROR: I/O error");
			e.printStackTrace();
		}
		if (fileContents == null) {
			return;
		}

		DBAccessor dbaccessor = new DBAccessor();
		dbaccessor.init();
		Connection conn = dbaccessor.getConnection();
		
		// TODO Prepare everything before updating or inserting
		if (conn != null) {
			
			PreparedStatement updatePreparedStatement = null;
			PreparedStatement insertPreparedStatement = null;
			try {
				
				String updateSql = "UPDATE worked SET salary = ? , cif_code = ?" +
								   "WHERE dni_number = ? and init_date = ?" ;
				updatePreparedStatement = conn.prepareStatement(updateSql);
				
				for (List<String> row : fileContents) {
					// TODO Update or insert record from WORKED for every row in file
					updatePreparedStatement.clearParameters();
					setUpdateParameters(updatePreparedStatement,row);
					if(0 == updatePreparedStatement.executeUpdate())
					{
						if(null == insertPreparedStatement)
						{
							String insertSql = "INSERT INTO worked(dni_number, cif_code, init_date, end_date, salary) VALUES(?, ?, ?, ?, ?)";
							insertPreparedStatement = conn.prepareStatement(insertSql);
						}
						insertPreparedStatement.clearParameters();
						setInsertParameters(insertPreparedStatement, row);
						insertPreparedStatement.executeUpdate();
					}
				}
				// TODO Validate transaction
				conn.commit();
				
				// TODO Close resources and check exceptions
			} catch (SQLException e) {
				System.err.println("ERROR: Executing sql");
				System.err.println(e.getMessage());
			} finally {
				if(updatePreparedStatement != null)
				{
					try {
						updatePreparedStatement.close();
					} catch (SQLException e) {
						System.err.println("ERROR: Closing statement");
						System.err.println(e.getMessage());
					}					
				}
				if(insertPreparedStatement != null)
				{
					try {
						insertPreparedStatement.close();
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
	
	private void setUpdateParameters(PreparedStatement updateStatement,
			List<String> row) throws SQLException {
		String[] rowArray = (String[]) row.toArray(new String[0]);
		
		setValueOrNull(updateStatement, 1, getIntegerFromStringOrNull(getValueIfNotNull(rowArray, 3))); // salary
		setValueOrNull(updateStatement, 2, getValueIfNotNull(rowArray, 1)); // cif_code
		setValueOrNull(updateStatement, 3, getValueIfNotNull(rowArray, 0)); // dni_number
		setValueOrNull(updateStatement, 4, getDateFromString(getValueIfNotNull(rowArray, 2))); // init_date
	}
	
	private void setInsertParameters(PreparedStatement updateStatement,
			List<String> row) throws SQLException {
		String[] rowArray = (String[]) row.toArray(new String[0]);
		setValueOrNull(updateStatement, 1, getValueIfNotNull(rowArray, 0)); // dni_number
		setValueOrNull(updateStatement, 2, getValueIfNotNull(rowArray, 1)); // cif_code
		setValueOrNull(updateStatement, 3, getDateFromString(getValueIfNotNull(rowArray, 2))); // init_date
		setValueOrNull(updateStatement, 4, (Date)null); // end_date
		setValueOrNull(updateStatement, 5, getIntegerFromStringOrNull(getValueIfNotNull(rowArray, 3))); // salary
	}
	
	private Integer getIntegerFromStringOrNull(String integer)
	{
		return (null !=integer) ? Integer.valueOf(integer) : null;
	}
	
	private Date getDateFromString(String date)
	{
		DateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
		try {
			return (Date)formatter.parse(date);
		} catch (ParseException e) {
			System.err.println("Error parsing date " + date + ". Returning null");
			return null;
		}		
	}

	private String getValueIfNotNull(String[] rowArray, int index) {
		String ret = null;
		if (index < rowArray.length) {
			ret = rowArray[index];
		}
		return ret;
	}

	private void setValueOrNull(PreparedStatement preparedStatement,
			int parameterIndex, Integer value) throws SQLException {
		if (value == null) {
			preparedStatement.setNull(parameterIndex, Types.INTEGER);
		} else {
			preparedStatement.setInt(parameterIndex, value);
		}
	}

	private void setValueOrNull(PreparedStatement preparedStatement,
			int parameterIndex, String value) throws SQLException {
		if (value == null || value.length() == 0) {
			preparedStatement.setNull(parameterIndex, Types.VARCHAR);
		} else {
			preparedStatement.setString(parameterIndex, value);
		}
	}

	private void setValueOrNull(PreparedStatement preparedStatement,
			int parameterIndex, java.util.Date value) throws SQLException {
		if (value == null) {
			preparedStatement.setNull(parameterIndex, Types.TIMESTAMP);
		} else {
			java.sql.Timestamp timestamp = new java.sql.Timestamp(
					value.getTime());
			preparedStatement.setTimestamp(parameterIndex, timestamp);
		}
	}
}
