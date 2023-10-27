package Main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class Base {

	Connection conn;
	
//	private void init() throws Exception { 	Noooooooo throws
//		String drv = "com.mysql.jdbc.Driver";
//		Class.forName(drv);
//		System.out.println("Driver cargado");
//	}
	
	private void init() throws Exception {
		String drv = "com.mysql.jdbc.Driver";
		Class.forName(drv);
		System.out.println("Driver cargado");
	}
	
	
	public void connect() throws SQLException {
		String addr = "127.0.0.1:3306"; // Cargamos direccion (localhost)
		String db = "asociacion_cervezera";
		String user = "user1";
		String pass = "user1pass";
		String url = "jdbc:mysql//" + addr + "/" + db; // url de la conexion 
		Connection conn = DriverManager.getConnection(url, user, pass); // Di no a los throws Exception
 		
	}
	
	private void queryCervezas() throws SQLException { 	// Consulta a la tabla cervezas
		String sql = "SELECT * FROM cerveza;";			// Canal para lanzar el query
		Statement st = conn.createStatement();			 
		ResultSet rs = st.executeQuery(sql);			// ResultSet guarda el res de la consulta y aqui se envia
		System.out.println("Consuta ejecutada correctamente.");
	}
	
	public void createTableEmpleado() {
		String sql = "CREATE TABLE )" + 
					 "	ID_empleado INT, " +
					 "	nombre VARCHAR(100) " +
					 "	PRIMARY KEY (ID_empleado)" +
					 ");";
		
	}
	
}
