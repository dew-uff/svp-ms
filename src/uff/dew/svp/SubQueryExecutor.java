package uff.dew.svp;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;

import uff.dew.avp.AVPConst;
import uff.dew.avp.localqueryprocessor.queryexecutor.QueryExecutor;
import uff.dew.svp.db.Database;
import uff.dew.svp.db.DatabaseException;
import uff.dew.svp.db.DatabaseFactory;
import uff.dew.svp.fragmentacaoVirtualSimples.Query;
import uff.dew.svp.fragmentacaoVirtualSimples.SubQuery;

/**
 * Object to execute a subquery originated from SVP technique
 * 
 * @author gabriel
 *
 */
public class SubQueryExecutor {

	private Query queryObj;
	private SubQuery subQueryObj;
	private String subQuery;
	private ExecutionContext context;
	private Database database;

	/**
	 * Creates a Executor object using the fragment originated in SVP technique
	 * 
	 * @param fragment
	 * @throws Exception
	 */
	public SubQueryExecutor(String fragment) throws SubQueryExecutionException {
		//System.out.println("SubQueryExecutor constructor: \n" + fragment);
		queryObj = new Query();
		subQueryObj = new SubQuery();

		try {
			subQuery = processFragment(fragment);
			//System.out.println(subQuery);

			if (subQuery.indexOf("order by") != -1) {
				subQuery = insertOrderByElementInSubQuery(subQuery);
				subQuery = removeOrderByFromSubquery(subQuery);
			}

			context = new ExecutionContext(fragment);

		}
		catch (IOException e) {
			throw new SubQueryExecutionException(e);
		}
	}

	/**
	 * Sets the local database information where the subquery will be executed
	 * 
	 * @param hostname
	 * @param port
	 * @param username
	 * @param password
	 * @param databaseName
	 * @param type
	 * @throws DatabaseException
	 */
	public void setDatabaseInfo(String hostname, int port, String username, String password,
			String databaseName, String type) throws DatabaseException {
		database = DatabaseFactory.getDatabase(hostname,port,username,password,databaseName,type);
		//        Catalog.get().setDatabaseObject(database);
	}

	/**
	 * Executes the subquery saving the results in the given OutputStream
	 * 
	 * @param os
	 * @throws Exception
	 */
	public boolean executeQuery(OutputStream os) throws SubQueryExecutionException {
		return SubQuery.executeSubQuery(subQuery, queryObj, subQueryObj, database, os);
	}

	//Luiz Matos
	//Passa IP e Porta do CQP para ser utilizado na gravacao do resultado na colecao temporaria
	public boolean executeQuery(boolean onlyCollectionStrategy, QueryExecutor qe) throws SubQueryExecutionException {
		BufferedReader configReader;
		String nodeName = "";
		
		try {
			configReader = new BufferedReader(new FileReader(AVPConst.CQP_CONF_FILE_NAME));
			String line;
			int lineCount = 1;

			while ((line = configReader.readLine()) != null) {
				line = line.trim();
				if (line.length() > 0) { // non-empty line
					if (line.charAt(0) != '#') {
						char fieldSeparator = ':';
						int separatorIndex;
						
						separatorIndex = line.indexOf(fieldSeparator);
						if ((separatorIndex <= 0)) {
							configReader.close();
							// no node address
							throw new IllegalArgumentException("Line " + lineCount + ": node address not informed");
						} else if (separatorIndex == line.length() - 1) {
							configReader.close();
							// no port number
							throw new IllegalArgumentException("Line " + lineCount + ": no port number");
							
						} else {
							nodeName = line.substring(0, separatorIndex).trim();
							// getting port numbers and connecting to CQP
						}
					} //fim do if (line.charAt(0) != '#')
				} // fim do if (line.length() > 0) { // non-empty line
				lineCount++;
			}
			configReader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return SubQuery.executeSubQuery(subQuery, queryObj, subQueryObj, database, onlyCollectionStrategy, nodeName, qe);
	}

	/**
	 * Returns the execution context of this query
	 * @return the execution context
	 */
	public ExecutionContext getExecutionContext() {
		return context;
	}

	private String processFragment(String fragment) throws IOException {
		//System.out.println("processFragment()");
		Query q = queryObj;
		SubQuery sbq = subQueryObj;

		StringReader sr = new StringReader(fragment);
		BufferedReader buff = new BufferedReader(sr);

		String line;
		String subquery = "";
		while((line = buff.readLine()) != null){    
			//System.out.println("line = " + line);
			if (!line.toUpperCase().contains("<GROUPBY>") && !line.toUpperCase().contains("<ORDERBY>") && !line.toUpperCase().contains("<ORDERBYTYPE>") && !line.toUpperCase().contains("<ELMTCONSTRUCT>") && !line.toUpperCase().contains("<AGRFUNC>")) {
				subquery = subquery + " " + line;
			}
			else {
				// obter as clausulas do groupby, orderby e de funcoes de agregacao
				if (line.toUpperCase().contains("<GROUPBY>")){
					String groupByClause = line.substring(line.indexOf("<GROUPBY>")+"<GROUPBY>".length(), line.indexOf("</GROUPBY>"));
					q.setGroupBy(groupByClause);
					//System.out.println("groupByClause = " + groupByClause);
				}

				if (line.toUpperCase().contains("<ORDERBY>")){
					String orderByClause = line.substring(line.indexOf("<ORDERBY>")+"<ORDERBY>".length(), line.indexOf("</ORDERBY>"));
					q.setOrderBy(orderByClause);
					//System.out.println("orderByClause = " + orderByClause);
				}

				if (line.toUpperCase().contains("<ORDERBYTYPE>")){
					String orderByType= line.substring(line.indexOf("<ORDERBYTYPE>")+"<ORDERBYTYPE>".length(), line.indexOf("</ORDERBYTYPE>"));                         
					q.setOrderByType(orderByType);
					//System.out.println("orderByType = " + orderByType);
				}

				if (line.toUpperCase().contains("<ELMTCONSTRUCT>")) {

					String elmtConstruct = line.substring(line.indexOf("<ELMTCONSTRUCT>")+"<ELMTCONSTRUCT>".length(), line.indexOf("</ELMTCONSTRUCT>"));

					if (!elmtConstruct.equals("") && !elmtConstruct.equals("{}")) {
						String[] constr = elmtConstruct.split(","); // separa todas os elementos utilizados no return statement (exceto os que contem funcao de agregacao)

						if (constr!=null) {

							for (String keyMap: constr) {

								String[] hashParts = keyMap.split("=");

								if (hashParts!=null) {
									q.setElmtConstructors(hashParts[0], hashParts[1]); // o par CHAVE, VALOR
									//System.out.println("hashParts[0], hashParts[1] = " + hashParts[0] + ", " + hashParts[1]);
								}
							}
						}
					}                       
				} 

				if (line.toUpperCase().contains("<AGRFUNC>")){ // soma 1 para excluir a tralha contida apos a tag <AGRFUNC>

					String aggregateFunctions = line.substring(line.indexOf("<AGRFUNC>")+"<AGRFUNC>".length(), line.indexOf("</AGRFUNC>"));

					if (!aggregateFunctions.equals("") && !aggregateFunctions.equals("{}")) {
						String[] functions = aggregateFunctions.split(","); // separa todas as funcoes de agregacao utilizadas no return statement.

						if (functions!=null) {

							for (String keyMap: functions) {

								String[] hashParts = keyMap.split("=");

								if (hashParts!=null) {

									q.setAggregateFunc(hashParts[0], hashParts[1]); // o par CHAVE, VALOR
									//System.out.println("hashParts[0], hashParts[1] = " + hashParts[0] + ", " + hashParts[1]);
								}
							}
						}
					}                       
				}                       
			}
		}

		subquery = subquery.trim();
		//System.out.println("subquery = " + subquery);
		sbq.setConstructorElement(SubQuery.getConstructorElement(subquery));
		return subquery;
	}

	private String insertOrderByElementInSubQuery(String subquery) {
		String orderByElement = getOrderByElementFromQuery(subquery);

		String beginElement = SubQuery.getElementAfterConstructor(subquery); // <element>
		String endElement = beginElement.replace("<", "</");

		int beginInsertPos = subquery.indexOf(beginElement);
		int endInsertPos = subquery.indexOf(endElement)+endElement.length();

		String wholeElement = subquery.substring(beginInsertPos,endInsertPos);

		subquery = subquery.substring(0,beginInsertPos) + "\r\n"
				+ "<orderby>"
				+ "<key>{"+orderByElement+"}</key>\r\n"
				+ "<element>" + wholeElement + "</element>\r\n"
				+ "</orderby>" +
				subquery.substring(endInsertPos);

		return subquery;
	}

	private static String getOrderByElementFromQuery(String query) {
		String orderBy = query;
		int orderByPos = orderBy.indexOf("order by") + "order by".length();
		orderBy = orderBy.substring(orderByPos).trim();
		int returnPos = orderBy.indexOf("return");
		orderBy = orderBy.substring(0, returnPos).trim();

		if (orderBy.indexOf("ascending") != -1) {
			int ascPos = orderBy.indexOf("ascending");
			orderBy = orderBy.substring(0,ascPos).trim();
		} else if (orderBy.indexOf("descending") != -1) {
			int descPos = orderBy.indexOf("descending");
			orderBy = orderBy.substring(0,descPos).trim();          
		}

		return orderBy;
	}

	private String removeOrderByFromSubquery(String query) {
		String orderBy = query;
		int orderByPos = orderBy.indexOf("order by");
		orderBy = orderBy.substring(orderByPos).trim();
		int returnPos = orderBy.indexOf("return");
		orderBy = orderBy.substring(0, returnPos).trim();

		return query.replace(orderBy, "");
	}

}
