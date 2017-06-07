package uff.dew.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import javax.xml.xquery.XQConnection;
import javax.xml.xquery.XQDataSource;
import javax.xml.xquery.XQException;
import javax.xml.xquery.XQExpression;
import javax.xml.xquery.XQPreparedExpression;
import javax.xml.xquery.XQResultSequence;

import net.xqj.basex.BaseXXQDataSource;
import uff.dew.avp.AVPConst;
import uff.dew.avp.commons.Utilities;
import uff.dew.svp.db.Database;
import uff.dew.svp.db.DatabaseException;
import uff.dew.svp.db.DatabaseFactory;

public class BaseXSTDalone {

	private int idQuery;
	private String originalQuery = null;
	private boolean storeResult;

	private FileWriter writer;
	private PrintWriter saida;
	
	protected XQDataSource dataSource;
    protected String databaseName;
	private XQConnection conn;

	public BaseXSTDalone(int idQuery) throws IllegalStateException, IllegalArgumentException, InterruptedException, Exception {
		setIdQuery(idQuery);
		//setStoreResult(storeResult);
		this.run();
	}

	public static void main(String[] args) {
		if( args.length != 1 ) {
			System.out.println("usage: java BaseXSTDalone idQuery" );
			return;
		}
		try {
			int idQuery = Integer.parseInt( args[0] );
			//boolean storeResult = Boolean.parseBoolean(args[1]);
			new BaseXSTDalone(idQuery);
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public void run() throws IllegalStateException, IllegalArgumentException, InterruptedException, Exception {

		try {
			//lê a consulta em disco e carrega seu conteúdo
			setOriginalQuery(Utilities.getFileContent(AVPConst.QUERIES_FILE_PATH_STDALONE, "q" + getIdQuery() + ".xq"));

			executeQuery(getOriginalQuery());

			System.gc();
			System.exit(0);

		} catch (Exception e) {
			System.err.println("BaseXSTDalone exception: " + e.getMessage());
			e.printStackTrace();
		}
	}

	public void executeQuery(String xquery) throws DatabaseException {

		XQResultSequence rs = null;
		XQResultSequence rs2 = null;

		//Database db = DatabaseFactory.getDatabase(AVPConst.DB_CONF_LOCALHOST, AVPConst.DB_CONF_PORT, AVPConst.DB_CONF_USERNAME, AVPConst.DB_CONF_PASSWORD, AVPConst.DB_CONF_DATABASE, AVPConst.DB_CONF_TYPE);
		
		createSession(AVPConst.DB_CONF_LOCALHOST, AVPConst.DB_CONF_PORT, AVPConst.DB_CONF_USERNAME, AVPConst.DB_CONF_PASSWORD, AVPConst.DB_CONF_DATABASE);
		
		try {

//			long startTime = System.nanoTime();
//			rs = db.executeQuery(xquery);
//			if(isStoreResult())
//				saveOutputFileSystem(rs);
//
//			long elapsedTime = System.nanoTime() - startTime;
//
			writer = new FileWriter(AVPConst.CQP_CONF_DIR_NAME + Utilities.createTimeResultFileNameCQP( getIdQuery(), "STDALONE", 1), true);
			saida = new PrintWriter(writer);
//			saida.print(elapsedTime/1000000+";");
//			
			long startTime2 = System.nanoTime();
			rs2 = baseExecuteQuery(xquery);
			//if(isStoreResult())
				saveOutputFileSystem(rs2);

			long elapsedTime2 = System.nanoTime() - startTime2;

			saida.println(elapsedTime2/1000000);
			
			saida.close();
			writer.close();

			//db.freeResources(rs);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (XQException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void saveOutputFileSystem(XQResultSequence rs) throws IOException, XQException {
		File fs = null;
		OutputStream out = new FileOutputStream(fs = new File(AVPConst.FINAL_RESULT_DIR + "finalResult.xml"));

		while (rs.next()) {
			String item = rs.getItemAsString(null);
			out.write(item.getBytes());
		}
		out.flush();
	}

	public void createSession(String host, int port, String username, String password, String database) {
		BaseXXQDataSource basexDataSource = new BaseXXQDataSource();

		basexDataSource.setServerName(host);
		basexDataSource.setPort(port);
		basexDataSource.setUser(username);
		basexDataSource.setPassword(password);
		setDatabaseName(database);

		dataSource = basexDataSource;
	}
	
	protected XQResultSequence baseExecuteQuery(String query) throws XQException {
    	//LOG.debug("executeQuery: " + query);
    	//long startTime = System.nanoTime();
		//String query2 = preprocess(query);
		
    	ExecutionContext qe = new ExecutionContext();
    	XQResultSequence result = qe.executeQuery(query);

    	//LOG.debug("QueryContext: " + result.hashCode());
    	//LOG.debug("Query execution time: " + (System.nanoTime() - startTime)/1000000 + "ms");
        return result;
    }
	
	private String preprocess(String query) {

		StringBuffer processedQuery = new StringBuffer();

		while (query.length() > 0) {
			int idx = query.indexOf("doc(");
			if (idx != -1) {
				// everything until 'doc('' is ok.
				processedQuery.append(query.substring(0,idx + 5));
				// remove doc( and the ' or " character after it
				query = query.substring(idx + 5);
				// remove ') or "), to get document name
				int idx2 = query.indexOf(')') - 1;
				String document = query.substring(0, idx2);
				// replace 'document.xml' by 'database/document.xml'
				processedQuery.append(getDatabaseName()+"/"+document);
				query = query.substring(idx2);
			} 
			else  {
				idx = query.indexOf("document(");
				if (idx != -1) {
					// everything until 'document('' is ok.
					processedQuery.append(query.substring(0,idx + 10));
					// remove document( and the ' or " character after it
					query = query.substring(idx+10);
					// remove ') or "), to get document name
					int idx2 = query.indexOf(')') - 1;
					String document = query.substring(0, idx2);
					// replace 'document.xml' by 'database/document.xml'
					processedQuery.append(getDatabaseName()+"/"+document);
					query = query.substring(idx2);
				}
				else {
					processedQuery.append(query);
					break;
				}
			}
		}

		return processedQuery.toString();
	}
	
	public int getIdQuery() {
		return idQuery;
	}

	public void setIdQuery(int idQuery) {
		this.idQuery = idQuery;
	}

	public String getOriginalQuery() {
		return originalQuery;
	}

	public void setOriginalQuery(String originalQuery) {
		this.originalQuery = originalQuery;
	}

	public boolean isStoreResult() {
		return storeResult;
	}

	public void setStoreResult(boolean storeResult) {
		this.storeResult = storeResult;
	}
	
    public String getDatabaseName() {
        return databaseName;
    }
    
    public void setDatabaseName(String name) {
        this.databaseName = name;
    }
    
    private class ExecutionContext {
		private XQPreparedExpression prepExp = null;
		private XQExpression exp = null;
		private XQResultSequence rs = null;
		private boolean closeSession = true;
		
		public XQResultSequence executeQuery(String query) throws XQException {
			closeSession = openSession();
			prepExp = conn.prepareExpression(query);
			rs = prepExp.executeQuery();
			
			return rs;
		}

		public void executeCommand(String command) throws XQException {
			closeSession = openSession();
			exp = conn.createExpression();
			exp.executeCommand(command);
		}

		
		public void close() throws XQException {
			if (rs != null) {
				rs.close();
			}
			if (prepExp != null) {
				prepExp.close();
			}
			if (exp != null) {
				exp.close();
			}
			if (closeSession) {
				closeSession();
			}
		}
	}
    
    protected boolean openSession() throws XQException {
		if (conn == null) {
			//LOG.debug("openSession");
			conn = dataSource.getConnection();
			return true;
		}
		return false;
	}
	
	protected void closeSession() throws XQException {
		//LOG.debug("closeSession");
		conn.close();
		conn = null;
	}
    
}
