package Main;

import java.sql.SQLException;

public class Main {
	
	public static void main (String[] args) {
	
		Base ej1 = new Base();
		
		try {
            //ej1.init();
            //ej1.connect();
            //ej1.queryCervezas();
            
			//ej1.createTableEmpleado();
            //ej1.updateNombresSocios();
            //ej1.getSocio(10);
            //ej1.getDBMetadata();
            //ej1.getTableMetadata();
            //ej1.insertaFoto(2, "HomerSimpson.jpg");
            //ej1.variosInserts();
            ej1.leerFichero("files/fichero.csv");
            ej1.disconnect();
        } catch (SQLException e) {
            System.err.println("Error al conectar a la base de datos.");
	
        }
	}

}
