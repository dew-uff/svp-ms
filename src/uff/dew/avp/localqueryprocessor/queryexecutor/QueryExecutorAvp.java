package uff.dew.avp.localqueryprocessor.queryexecutor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.rmi.RemoteException;

import uff.dew.avp.AVPConst;
import uff.dew.avp.commons.LocalQueryTaskStatistics;
import uff.dew.avp.commons.Logger;
import uff.dew.avp.connection.DBConnectionPoolEngine;
import uff.dew.avp.localqueryprocessor.localquerytask.LocalQueryTask;
import uff.dew.avp.localqueryprocessor.dynamicrangegenerator.PartitionSize;
import uff.dew.avp.localqueryprocessor.dynamicrangegenerator.PartitionTuner;
import uff.dew.avp.localqueryprocessor.dynamicrangegenerator.PartitionTunerMT_NonUniform;
import uff.dew.avp.localqueryprocessor.dynamicrangegenerator.Range;
import uff.dew.avp.localqueryprocessor.dynamicrangegenerator.RangeStatistics;
import uff.dew.svp.SubQueryExecutionException;
import uff.dew.svp.SubQueryExecutor;
import uff.dew.svp.db.DatabaseException;


public class QueryExecutorAvp extends QueryExecutor {

	private Logger logger = Logger.getLogger(QueryExecutorAvp.class);
	private PartitionTuner partitionTuner;
	private PartitionSize currentPartitionSize;
	private int nextRangeValue;
	private Preview preview;

	private OutputStream out = null;
	private String filename = null;
	private FileOutputStream filepath = null;
	private File fs = null;

	public QueryExecutorAvp(LocalQueryTask lqt, DBConnectionPoolEngine dbpool, String query, Range range,
			RangeStatistics statistics, LocalQueryTaskStatistics lqtStatistics, boolean onlyCollectionStrategy, String tempCollectionName) throws RemoteException {
		super(lqt, dbpool, query, range, lqtStatistics, onlyCollectionStrategy, tempCollectionName);
		//this.partitionTuner = new PartitionTunerMT_NonUniform(this.range.getStatistics());
		partitionTuner = new PartitionTunerMT_NonUniform(range.getVPSize(), statistics);

		currentPartitionSize = null;
		if (this.lqtStatistics != null)
			this.lqtStatistics.setPartitionTuner(this.partitionTuner);

		preview = new Preview(range.getFirstValue(),range.getOriginalLastValue());
		System.out.println("QueryExecutorAvp constructor ...");
	}

	protected boolean getQueryLimits(String query, int[] limits) {
		switch (state) {
		case ST_STARTING_RANGE: {
			limits[0] = range.getFirstValue();
			//limits[0] = Integer.parseInt(SubQuery.getIntervalBeginning(query));
			state = ST_PROCESSING_RANGE;
			currentPartitionSize = partitionTuner.getPartitionSize();
			break;
		}
		case ST_PROCESSING_RANGE: {
			limits[0] = nextRangeValue;
			if (partitionTuner.stillTuning() && currentPartitionSize.getNumPerformedExecutions() >= currentPartitionSize.getNumExpectedExecutions()) {
				// Number of expected executions was reached.
				// Send feedback to partition tuner.
				partitionTuner.setSizeResults(currentPartitionSize);
				// Ask a new partition size.
				currentPartitionSize = partitionTuner.getPartitionSize();
			}
			break;
		}
		default: {
			throw new IllegalThreadStateException("LocalQueryTaskEngine_AVP Exception: getQueryLimits() should not be called while in state " + state + "!");
		}
		}
		limits[1] = range.getNextValue(currentPartitionSize.numberOfKeys());
		//limits[1] = Integer.parseInt(SubQuery.getIntervalEnding(query));
		nextRangeValue = limits[1];
		if (limits[0] == limits[1]) {
			state = ST_RANGE_PROCESSED;

			/*
			 * if ( a_partitionTuner.stillTuning() ) { if(
			 * a_currentPartitionSize.getNumPerformedExecutions() > 0 ) { //
			 * Executions were performed. // Send feedback to partition tuner.
			 * a_partitionTuner.setSizeResults( a_currentPartitionSize ); // Ask
			 * a new partition size. a_currentPartitionSize =
			 * a_partitionTuner.getPartitionSize(); } }
			 */
			partitionTuner.reset();
			return false;
		} else if (limits[0] < limits[1])
			return true;
		else
			throw new IllegalThreadStateException("LocalQueryTaskEngine_AVP Exception: lower limit superior to upper!");
	}

	protected void executeSubQuery(String query, int[] limit)
			throws RemoteException, SubQueryExecutionException, DatabaseException {
		//System.out.println("Entrei no método executeSubQuery() da classe QueryExecutorAvp");

		boolean hasResults = false;

		//Insere subintervalos no lugar do ? na subquery
		while(query.indexOf("?") > -1) {
			query = query.replaceFirst("\\?",limit[0]+"");
			query = query.replaceFirst("\\?",limit[1]+"");
		}  
		//System.out.println("Query transformada: " + query);
		int aux = limit[1]-limit[0];
		//System.out.println("range: " + limit[0] + " - " + limit[1] + " ("+aux+" positions)");

		//FINALMENTE A EXECUÇÃO DA CONSULTA

		//Execução da subconsulta usando svp_lib
		SubQueryExecutor sqe = new SubQueryExecutor(query);

		sqe.setDatabaseInfo(AVPConst.DB_CONF_LOCALHOST, AVPConst.DB_CONF_PORT, AVPConst.DB_CONF_USERNAME,
				AVPConst.DB_CONF_PASSWORD, AVPConst.DB_CONF_DATABASE, AVPConst.DB_CONF_TYPE);

		filename = AVPConst.PARTIAL_RESULTS_FILE_PATH + "/partial_" + limit[0] + ".xml";
		try {
			out = new FileOutputStream(fs = new File(filename));

			// execute query, saving result to a partial file in local fs
			long startTime = System.nanoTime();
			hasResults = sqe.executeQuery(out);
			long elapsedTime = System.nanoTime() - startTime;

			this.setTimeProcessingSubqueries(elapsedTime / 1000000); // em ms
			System.out.println("Time elapsed to process the subquery = " + this.getTimeProcessingSubqueries() + "ms");

			out.flush();
			out.close();
			out = null;

			// if it doesn't have results, delete the partial file
			if (!hasResults) {
				fs.delete();
			}
			this.setCompleteFileName(filename);
			currentPartitionSize.setExecTime(elapsedTime / 1000000); //aqui é registrado o desempenho (tempo) do proc. do fragmento
			preview.setRange(nextRangeValue); //obtém novo intervalo
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	class Preview {
		private long init;
		private int rangeBegin;
		private int rangeEnd;
		private int actual;

		public Preview(int rangeBegin, int rangeEnd) {
			this.rangeBegin = rangeBegin;
			this.rangeEnd = rangeEnd;
			actual = rangeBegin;
			init = System.currentTimeMillis();
		}

		public void setRange(int range) {
			actual = range;
		}

		public String toString() {
			float tr = (rangeEnd-rangeBegin+1);
			float at = System.currentTimeMillis()-init;
			float ar = actual-rangeBegin+1;
			long total = (long)((tr * at)/ar);
			return "Estimated time: "+at+"/"+total+"ms";
		}
	}

}
