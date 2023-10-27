package series;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class SeriesDatabase {
	
	//Atributos
	private static Connection conn = null;
	private Statement st = null;
	private ResultSet rs = null;
	private PreparedStatement pst = null;
	
	public SeriesDatabase() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			System.out.println("No se ha podido encontrar el Driver");
		}
	}

	private void cerrarRecursos() {
		try {
			System.out.println("Cierre de recursos \n");
			if (st != null)
				st.close();
			if (rs != null)
				rs.close();
			if (pst != null)
				pst.close();
			if (conn != null)
				conn.close();
		} catch (SQLException e) {
			System.err.println("Error al cerrar los recursos");
			System.err.println(e.getMessage());
		}
	}

	public boolean openConnection() {
		boolean connected = false;
		try {
			if (conn == null || conn.isClosed()) {
				String addr = "127.0.0.1:3306";
				String db = "series";
				String user = "series_user";
				String pass = "series_pass";
				String url = "jdbc:mysql://" + addr + "/" + db;
				conn = DriverManager.getConnection(url, user, pass);
				System.out.println("Conexión establecida");
				connected = true;
			}
		} catch (SQLException e) {
			System.err.println("La conexión no ha podido ser abierta");
			System.err.println(e.getMessage());
		}
		return connected;
	}

	public boolean closeConnection() {
		boolean disconnected = false;
		try {
			if (conn != null) {
				conn.close();
				System.out.println("Conexión finalizada");
				disconnected = true;
			}
		} catch (SQLException e) {
			System.err.println("Error en la desconexión");
			System.err.println(e.getMessage());
		}
		return disconnected;
	}

	public boolean createTableCapitulo() {
		this.openConnection();
		boolean tableCapCreated = false;
		try {

			String sql = "CREATE TABLE capitulo (" 
					+ "  n_orden INT," 
					+ "  titulo VARCHAR(100)," 
					+ "  duracion INT,"
					+ "  fecha DATE," 
					+ "  n_temporada INT," 
					+ "  id_serie INT,"
					+ "  PRIMARY KEY (n_orden, n_temporada, id_serie),"
					+ "  FOREIGN KEY (id_serie, n_temporada) REFERENCES temporada (id_serie, n_temporada)"
					+ "  ON DELETE CASCADE ON UPDATE CASCADE" 
					+ ");";

			st = conn.createStatement();
			int result = st.executeUpdate(sql);
			if (result == 0) {
				System.out.println("Tabla capitulo creada");
				tableCapCreated = true;
			}
		} catch (SQLException e) {
			System.err.println("La tabla capitulo no ha podido ser creada");
			System.err.println(e.getMessage());
		} finally {
			cerrarRecursos();
		}
		return tableCapCreated;
	}

	public boolean createTableValora() {
		this.openConnection();
		boolean tableValCreated = false;
		try {

			String sql = "CREATE TABLE valora (" 
					+ "  fecha DATE," 
					+ "  valor INT," 
					+ "  id_serie INT,"
					+ "  n_temporada INT," 
					+ "  n_orden INT," 
					+ "  id_usuario INT,"
					+ "  PRIMARY KEY (fecha, n_orden, n_temporada, id_serie, id_usuario),"
					+ "  FOREIGN KEY (n_orden, n_temporada, id_serie) REFERENCES capitulo (n_orden, n_temporada, id_serie)"
					+ "  ON DELETE CASCADE ON UPDATE CASCADE,"
					+ "  FOREIGN KEY (id_usuario) REFERENCES usuario (id_usuario)"
					+ "  ON DELETE CASCADE ON UPDATE CASCADE" 
					+ ");";

			st = conn.createStatement();
			int result = st.executeUpdate(sql);
			if (result == 0) {
				System.out.println("Tabla capitulo creada");
				tableValCreated = true;
			}
		} catch (SQLException e) {
			System.err.println("La tabla valora no ha podido ser creada");
			System.err.println(e.getMessage());
		} finally {
			cerrarRecursos();
		}
		return tableValCreated;
	}

	public int loadCapitulos(String fileName) {
		this.openConnection();
		int count = 0;
		BufferedReader buffer = null;
		try {
			buffer = new BufferedReader(new FileReader(fileName));
			String line = null;
			conn.setAutoCommit(false);
			try {
				String query = "INSERT INTO capitulo (id_serie, n_temporada, n_orden, fecha, titulo, duracion) VALUES (?,?,?,?,?,?);";
				buffer.readLine(); //skip header
				while ((line = buffer.readLine()) != null) {
					String[] dato = line.split(";");
					
					pst = conn.prepareStatement(query);

					pst.setString(1, dato[0]);
					pst.setString(2, dato[1]);
					pst.setString(3, dato[2]);
//					String fecha = dato[3];
//					Date sqlfecha = Date.valueOf(fecha);
//					pst.setDate(4, sqlfecha);
					pst.setString(4, dato[3]);
					pst.setString(5, dato[4]);
					pst.setString(6, dato[5]);
					pst.executeUpdate();
					
					System.out.println("Insertado correctamente");
					count++;
				}
				conn.commit();
			} catch (IOException e) {
				System.err.println("Error al introducir el fichero");
				System.err.println(e.getMessage());
			}
		} catch (FileNotFoundException e) {
			System.err.println("No se pudo encontrar el fichero");
			System.err.println(e.getMessage());
		} catch (SQLException e) {
			System.err.println("No se pudo leer correctamente el fichero");
			System.err.println(e.getMessage());
			try {
				conn.setAutoCommit(false);
				conn.rollback();
				count = 0;
			} catch (SQLException e1) {
				System.err.println(e1.getMessage());
			}
		} finally {
			try {
				if (buffer != null)
					buffer.close();
			} catch (IOException e) {
				System.err.println("Error al cerrar el fichero");
			}
			cerrarRecursos();
		}
		return count;
	}

	public int loadValoraciones(String fileName) {
		this.openConnection();
		int count = 0;
		BufferedReader buffer = null;
		try {
			buffer = new BufferedReader(new FileReader(fileName));
			String line = null;
			conn.setAutoCommit(true);
			try {
				String query = "INSERT INTO valora (id_serie, n_temporada, n_orden, id_usuario, fecha, valor) VALUES (?,?,?,?,?,?);";
				line = null;
				buffer.readLine(); //skip header
				while ((line = buffer.readLine()) != null) {
					String[] dato = line.split(";");
					
					pst = conn.prepareStatement(query);

					pst.setString(1, dato[0]);
					pst.setString(2, dato[1]);
					pst.setString(3, dato[2]);
					pst.setString(4, dato[3]);
					pst.setString(5, dato[4]);
					pst.setString(6, dato[5]);
					
					
					
					pst.executeUpdate();
					
					System.out.println("Insertado correctamente");
					count++;
				}
			} catch (IOException e) {
				System.err.println("Error al introducir el fichero");
				System.err.println(e.getMessage());
			}
		} catch (FileNotFoundException e) {
			System.err.println("No se pudo encontrar el fichero");
			System.err.println(e.getMessage());
		} catch (SQLException e) {
			System.err.println("No se pudo leer correctamente la linea: " + count + 1);
			System.err.println(e.getMessage());
		} finally {
			try {
				if (buffer != null)
					buffer.close();
			} catch (IOException e) {
				System.err.println("Error al cerrar el fichero");
			}
			cerrarRecursos();
		}
		return count;
	}

	public String catalogo() {
		this.openConnection();
		
		String query = "SELECT temporada.id_serie, serie.titulo, temporada.n_temporada, temporada.n_capitulos " +
						"  FROM temporada " +
						"  INNER JOIN serie ON temporada.id_serie = serie.id_serie " +
						"  ORDER BY temporada.n_temporada, temporada.id_serie ;";
		
		
		try {
			String info = "{";
			
			st = conn.createStatement();
			rs = st.executeQuery(query);
			
			boolean primer_query = true;
			int pre_id_serie = -1;
			int pre_n_temporada = -1; 
			int pre_n_capitulo = -1;
		
			while(rs.next()) {
				int id_serie = rs.getInt("id_serie");
				String titulo = rs.getString("titulo");
				int n_temporada = rs.getInt("n_temporada");
				int n_capitulo = rs.getInt("n_capitulos");

				if(id_serie > -1)
				{
					if (pre_id_serie != id_serie && primer_query)
					{
						primer_query = false;
						info += titulo + ":[";
						pre_id_serie = id_serie;
					}	
					
					else if(!primer_query)
					{
						if(n_temporada != pre_n_temporada)
							info += n_capitulo + ",";
						
						pre_n_temporada = n_temporada;
						if(id_serie != pre_id_serie)
						{
							if(info.charAt(info.length() - 1) == ',')
								info = info.substring(0, info.length() - 1);
							info += "],";
							primer_query = true;
						}
						pre_id_serie = id_serie;
					}
					pre_n_capitulo = n_capitulo;
						
				}
				
			}
			if(info.charAt(info.length() - 1) == ',')				// Caso normal, borra la ultima ','
			{														// y pone } al final
				info = info.substring(0, info.length() - 1);
				info += "}";
			}
			else if(info.charAt(info.length() - 1) == '{')			// Caso en el que no hay nada
				info += "}";
			else													// Caso hay titulo pero no temporadas
				info += "]}";
			return info.replaceAll(" ", "_");
		} catch (SQLException e){
			System.err.println("Error al ejecutar la consulta del catalogo");
			System.err.println(e.getMessage());
			return null;
		} catch (Exception e){
			System.err.println("Se produjo un error al ejecutar la consulta del catalogo");
			System.err.println(e.getMessage());
			return null;
		} finally {
			cerrarRecursos();
		}
	}

	public String noHanComentado() {
		this.openConnection();
		String query = "SELECT usuario.id_usuario, usuario.nombre, usuario.apellido1, usuario.apellido2, comenta.texto "
				+ "FROM usuario " 
				+ "LEFT JOIN comenta ON usuario.id_usuario = comenta.id_usuario "
				+ "ORDER BY usuario.apellido1, usuario.apellido2, usuario.nombre ;";

		try {
			LinkedHashMap<Integer, String> haComentado = new LinkedHashMap<>();
			LinkedHashMap<Integer, String> noHaComentado = new LinkedHashMap<>();
			String info = "[";
			st = conn.createStatement();
			rs = st.executeQuery(query);

			while (rs.next()) {
				
				int id_usuario = rs.getInt("id_usuario");
				String apellido1 = rs.getString("apellido1");
				String apellido2 = rs.getString("apellido2");
				String nombre = rs.getString("nombre");
				String usuario = nombre + " " + apellido1 + " " + apellido2 + ", ";
				
				if (rs.getString("texto") != null && !haComentado.containsKey(id_usuario)) {
					haComentado.put(id_usuario, usuario);
				}
			}
			
			if (st != null)
				st.close();
			if (rs != null)
				rs.close();
			st = conn.createStatement();
			rs = st.executeQuery(query);
			
			while (rs.next()) {
				
				int id_usuario = rs.getInt("id_usuario");
				String apellido1 = rs.getString("apellido1");
				String apellido2 = rs.getString("apellido2");
				String nombre = rs.getString("nombre");
				String usuario = nombre + " " + apellido1 + " " + apellido2 + ", ";
				
				if (!haComentado.containsKey(id_usuario) && !noHaComentado.containsKey(id_usuario)) {
					info += usuario;
					noHaComentado.put(id_usuario, usuario);
				}
			}
			
			return info = info.substring(0, info.length() - 2) + "]";
			
		} catch (SQLException e) {
			System.err.println("Error al ejecutar la consulta de los comentarios");
			System.err.println(e.getMessage());
			return null;
		} catch (Exception e) {
			System.err.println("Se produjo un error al ejecutar la consulta del catalogo");
			System.err.println(e.getMessage());
			return null;
		} finally {
			cerrarRecursos();
		}
	}

	public double mediaGenero(String genero) {
		this.openConnection();
		
		String query1 = "  SELECT COUNT(DISTINCT descripcion) id_genero" +
						"  FROM genero " +
						"  WHERE descripcion = ? ;";
		
		String query2 = "SELECT COUNT(valora.id_serie) capitulos, AVG(valor) media " +
						"FROM valora v " +
						"  INNER JOIN serie s ON s.id_serie = v.id_serie " +
						"  INNER JOIN pertenece p ON p.id_serie = v.id_serie " +
						"  INNER JOIN genero g ON p.id_genero = g.id_genero " +
						"WHERE g.descripcion = ? ;";
		
		try {
			pst = conn.prepareStatement(query1);
			pst.setString(1, genero);
			rs = pst.executeQuery();
			
			if(rs.next())
					System.out.println("Nombre del genero " + rs.getString("genero"));
			else
					System.out.println("No existe ese genero");
			
			int n_genero = rs.getInt("id_genero");
			pst.close();
			rs.close();
			
			try {
				if(n_genero > 0)
				{
					pst = conn.prepareStatement(query2);
					pst.setString(1, genero);
					rs = pst.executeQuery();
					rs.next();
					float media = rs.getFloat("media");
					int capitulos = rs.getInt("capitulos");
					
					if(capitulos <= 0)
						return 0.0;
					return media;
				}	
			} catch (Exception e) {
				System.err.println("No existe el genero");
				System.err.println(e.getMessage());
			}
			
		} catch (SQLException e) {
			System.out.println("Mensaje de error: " + e.getMessage());
			System.out.println("Codigo de error: " + e.getErrorCode());
			System.out.println("Estado SQL: " + e.getSQLState());
			
			return -2;
		} catch (Exception e) {
			return -1;
		} finally {
			cerrarRecursos();
		}
		return 0.0;
	}

	public double duracionMedia(String idioma) {
        this.openConnection();

        String query = "  SELECT capitulo.n_orden, capitulo.titulo, capitulo.duracion, valora.valor, serie.idioma " 
                + "  FROM capitulo "
                + "  INNER JOIN valora ON capitulo.n_orden = valora.n_orden "
                + "  INNER JOIN serie ON capitulo.id_serie = serie.id_serie "
                + "  WHERE idioma = ? ;";

        try {
            pst = conn.prepareStatement(query);
            pst.setString(1, idioma);
            rs = pst.executeQuery();
            double duracionTotal = 0;
            int count = 0;
            LinkedHashMap<Integer, String> valorizado = new LinkedHashMap<>();
            LinkedHashMap<Integer, String> noValorizado = new LinkedHashMap<>();

            while (rs.next()) {
                if (!eqNull(rs.getInt("valor"), null)) {
                    valorizado.put(rs.getInt("capitulo.n_orden"), rs.getString("capitulo.titulo"));
                }
            }

            if (st != null)
                st.close();
            if (rs != null)
                rs.close();
            st = conn.createStatement();
            rs = st.executeQuery(query);

            while (rs.next()) {
                if (!valorizado.containsKey(rs.getInt("capitulo.n_orden")) && !noValorizado.containsKey(rs.getInt("capitulo.n_orden"))) {
                    duracionTotal += rs.getInt("capitulo.duracion");
                    count++;
                    noValorizado.put(rs.getInt("capitulo.n_orden"), rs.getString("capitulo.titulo"));
                }
            }
            if (noValorizado.isEmpty())
                return -1;
            return duracionTotal / count;
        } catch (SQLException e) {
            return -2;
        } finally {
            cerrarRecursos();
        }
    }

	private static boolean eqNull(Object o1, Object o2) {
        return o1 == o2 || o1 != null && o1.equals(o2);
	}

	public boolean setFoto(String filename) {
		this.openConnection();
		FileInputStream fis = null;
		
		String query =	"  UPDATE usuario " +
						"  SET fotografia = ? " +
						"  WHERE apellido1 = 'Cabeza' ;";
		
		try {
			pst = conn.prepareStatement(query);
			File foto = new File("" + filename); // "" la imagen se encuentra en la raiz del proyecto
			fis = new FileInputStream(foto);
			pst.setBinaryStream(1, fis, (int)foto.length());
			
			int n = pst.executeUpdate();
			if(n > 0)
				System.out.println("Foto cambiada correctamente.");
			else
				System.out.println("No se ha podido cambiar la foto.");	
			return true;
			
		} catch (SQLException e) {
			System.out.println("Mensaje de error: " + e.getMessage());
			System.out.println("Codigo de error: " + e.getErrorCode());
			System.out.println("Estado SQL: " + e.getSQLState());
		} catch (FileNotFoundException e){
			System.err.println("Error no se encontro la foto");
			System.err.println(e.getMessage());
		} catch (Exception e) {
			System.err.println("Error desconocido al cambiar la foto");
			System.err.println(e.getMessage());
		} finally {
			try {
				if(pst != null)
					pst.close();
				if(fis != null)
					fis.close();
			} catch (SQLException e) {
				System.err.println("Error al cerrar las estructuras.");
				System.err.println(e.getMessage());
			} catch (IOException e) {
				System.err.println("Error al cerrar el fichero");
				System.err.println(e.getMessage());
			}
		}
		return false;
	}

}
