package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.actors.profiling.tasks.INDTask;
import de.ddm.actors.profiling.tasks.UniqueColumnTask;
import de.ddm.actors.profiling.tasks.WorkTask;
import de.ddm.configuration.SystemConfiguration;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import scala.Array;
import scala.Int;

import java.io.File;
import java.util.*;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<String[]> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		ActorRef<LargeMessageProxy.Message> dependencyWorkerLargeMessageProxy;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ColumnCreationMessage implements Message {
		private static final long serialVersionUID = -7642422259675583598L;
		ActorRef<ColumnCreator.Message> columnCreator;
		ArrayList<UniqueColumnTask> taskList;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class UniqueColumnToMinerMessage implements Message {
		private static final long serialVersionUID = -7642422259678734598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		ArrayList<String> data;
		int tableIndex;
		int columnIndex;
		int taskId;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class INDToMinerMessage implements Message {
		private static final long serialVersionUID = -7642422259672222598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		int c1TableIndex;
		int c1ColumnIndex;
		int c2TableIndex;
		int c2ColumnIndex;
		boolean isDependantFirst;
		boolean isDependantSecond;
		int taskId;
	}

	@Getter
	@NoArgsConstructor
	public static class ShutDownMinerMessage implements Message {
		private static final long serialVersionUID = -5242338806092192722L;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];

		this.inputReaders = new ArrayList<>(inputFiles.length);
		this.originalFileContents = new ArrayList[inputFiles.length];
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.workers = new HashMap<>();

		this.busyWorkers = new ArrayList<>();
		this.idleWorkers = new ArrayList<>();
		this.columnCreators = new ArrayList<>();

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;

	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final String[][] headerLines;
	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private final Map<ActorRef<DependencyWorker.Message>,ActorRef<LargeMessageProxy.Message>> workers;
	private final List<ActorRef<DependencyWorker.Message>> busyWorkers;
	private final List<ActorRef<DependencyWorker.Message>> idleWorkers;
	// custom
	private List<WorkTask> workList = new ArrayList<>();
	private Map<Integer, ActorRef<DependencyWorker.Message>> busyWorkList = new HashMap<>();
	private List<ActorRef<ColumnCreator.Message>> columnCreators;

	private int filesRead = 0;
	private ArrayList<String[]>[] originalFileContents;
	// all columns with their origin [table-index][column-index] as key
	private final ArrayList<Map<Integer, ArrayList<String>>> columns = new ArrayList<>();

	private final ArrayList<Integer[]> pairs = new ArrayList<>();

	// test
	private final ArrayList<Integer[]> allINDs = new ArrayList<>();
	private int indTasks = 0;

	private Map<Integer, WorkTask> taskIdMap = new HashMap<>();


	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(ColumnCreationMessage.class, this::handle)
				.onMessage(UniqueColumnToMinerMessage.class, this::handle)
				.onMessage(INDToMinerMessage.class, this::handle)
				.onMessage(ShutDownMinerMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		this.startTime = System.currentTimeMillis();
		return this;
	}

	static int headerLinesRead = 0;
	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
		DependencyMiner.headerLinesRead++;
		if(DependencyMiner.headerLinesRead == this.inputFiles.length){
			createPairCandidates();
		}
		return this;
	}

	private Behavior<Message> handle(BatchMessage message) {
		if (this.originalFileContents[0] == null){
			for (int i = 0; i < 7; i++) {
				this.originalFileContents[i] = new ArrayList<>();
				this.columns.add(new HashMap<Integer, ArrayList<String>>());
			}
		}
		if (!message.getBatch().isEmpty()){
			this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
			this.originalFileContents[message.getId()].addAll(message.getBatch());
		} else {
			this.getContext().getLog().info(this.originalFileContents[message.getId()].size() + " entries loaded " +
					"from table " + message.getId() + ". Spawning ColumnCreator Actor");
			this.columnCreators.add(getContext().spawn(ColumnCreator.create(message.getId(), this.originalFileContents[message.getId()]), ColumnCreator.DEFAULT_NAME + "_" + message.getId()));
			this.columnCreators.get(this.columnCreators.size() - 1).tell(new ColumnCreator.CreateColumnsMessage(this.getContext().getSelf()));
			this.getContext().stop(this.inputReaders.get(message.getId()));

		}
		return this;
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		ActorRef<LargeMessageProxy.Message> dependencyWorkerLargeMessageProxy = message.getDependencyWorkerLargeMessageProxy();

		if(this.workers.containsKey(dependencyWorker))
			return this;

		this.workers.put(dependencyWorker, dependencyWorkerLargeMessageProxy);
		this.getContext().watch(dependencyWorker);

		if(this.workList.isEmpty()){
			this.idleWorkers.add(dependencyWorker);
			return this;
		}
		this.idleWorkers.add(dependencyWorker);
		assignTasksToWorkers();
		return this;
	}

	private void createINDTasks() {
		ArrayList<Integer[]> readyPairs = new ArrayList<>();
		for(Integer[] pair: this.pairs) {
			if(pair[4] == 1 && pair[5] == 1) {
				String name = this.inputFiles[pair[0]].getName() + " -> " + this.inputFiles[pair[2]].getName() + ": [" +
						this.headerLines[pair[0]][pair[1]] + "] c [" + this.headerLines[pair[2]][pair[3]] + "]";
				this.workList.add(new INDTask(this.columns.get(pair[0]).get(pair[1]), this.columns.get(pair[2]).get(pair[3]),
						pair[0], pair[1], pair[2], pair[3], name));
				// test
				this.indTasks += 1;
				readyPairs.add(pair);
			}
		}
		this.getContext().getLog().info("Created " + readyPairs.size() + " IND tasks.");
		this.pairs.removeAll(readyPairs);
	}

	private Behavior<Message> handle(ColumnCreationMessage message) {
		this.getContext().getLog().info("Received " + message.taskList.size() + " columns from table " + message.taskList.get(0).getTableIndex());
		this.columnCreators.remove(message.columnCreator);
		this.getContext().stop(message.getColumnCreator());
		this.filesRead += 1;
		// save columns
		for (UniqueColumnTask t: message.getTaskList()) {
			this.columns.get(t.getTableIndex()).put(t.getColumnIndex(), t.getData());
		}
		this.workList.addAll(message.getTaskList());

		if(this.filesRead == this.inputFiles.length) {
			this.originalFileContents = null;
		}

		assignTasksToWorkers();
		return this;
	}

	private void createPairCandidates(){
		for (int i = 0; i < this.headerLines.length;i++){
			// create only one direction
			for (int j = i; j < this.headerLines.length; j++){
				createPairsFromTables(i, j);
			}
		}
	}

	private void createPairsFromTables(int table1, int table2){
		for (int i = 0; i < this.headerLines[table1].length; i++){
			for (int j = 0; j < this.headerLines[table2].length;j++){
				if(table1 == table2 && i >= j){
					continue;
				}
				this.pairs.add(new Integer[]{table1, i, table2, j, 0, 0});
			}
		}
	}

	private Behavior<Message> handle(UniqueColumnToMinerMessage message) {
		this.getContext().getLog().info("Received " + message.getData().size() + " unique values for table " + this.inputFiles[message.tableIndex].getName() + " column " + this.headerLines[message.tableIndex][message.columnIndex]);
		double before = this.columns.get(message.tableIndex).get(message.columnIndex).size();
		this.getContext().getLog().info(message.getData().size() + " uniques of "+before+" - reduced to "+ ((double)message.getData().size()) / before * 100.0 + " percent");
		this.columns.get(message.tableIndex).put(message.columnIndex, message.data);
		this.busyWorkers.remove(message.getDependencyWorker());
		this.idleWorkers.add(message.getDependencyWorker());
		this.busyWorkList.remove(message.taskId);

		setColumnReady(message.tableIndex, message.columnIndex);

		createINDTasks();

		assignTasksToWorkers();
		return this;
	}

	private void setColumnReady(int t, int c) {
		for(Integer[] pair: this.pairs) {
			if (pair[0] == t && pair[1] == c)
				pair[4] = 1;
			if (pair[2] == t && pair[3] == c)
				pair[5] = 1;
		}
	}

	private void checkTransitiveINDs() {
		//TODO
	}

	private Behavior<Message> handle(INDToMinerMessage message) {
		if(message.isDependantFirst() || message.isDependantSecond()){
			ArrayList<InclusionDependency> resultList = new ArrayList<>();
			if (message.isDependantFirst){
				this.getContext().getLog().info("Received IND: " + this.inputFiles[message.c1TableIndex].getName() + "." + this.headerLines[message.c1TableIndex][message.c1ColumnIndex] + " -> "
						+ this.inputFiles[message.c2TableIndex].getName() + "." + this.headerLines[message.c2TableIndex][message.c2ColumnIndex]);
				InclusionDependency ind = new InclusionDependency(this.inputFiles[message.getC2TableIndex()],
						new String[]{this.headerLines[message.getC2TableIndex()][message.c2ColumnIndex]},
						this.inputFiles[message.getC1TableIndex()],
						new String[]{this.headerLines[message.getC1TableIndex()][message.c1ColumnIndex]});
				resultList.add(ind);
				this.allINDs.add(new Integer[]{message.c1TableIndex, message.c1ColumnIndex, message.c2TableIndex, message.c2ColumnIndex});
				checkTransitiveINDs();
			}
			if (message.isDependantSecond){
				this.getContext().getLog().info("Received IND: " + this.inputFiles[message.c2TableIndex].getName() + "." + this.headerLines[message.c2TableIndex][message.c2ColumnIndex] + " -> "
						+ this.inputFiles[message.c1TableIndex].getName() + "." + this.headerLines[message.c1TableIndex][message.c1ColumnIndex]);
				InclusionDependency ind = new InclusionDependency(this.inputFiles[message.getC1TableIndex()],
						new String[]{this.headerLines[message.getC1TableIndex()][message.c1ColumnIndex]},
						this.inputFiles[message.getC2TableIndex()],
						new String[]{this.headerLines[message.getC2TableIndex()][message.c2ColumnIndex]});
				resultList.add(ind);
				this.allINDs.add(new Integer[]{message.c1TableIndex, message.c1ColumnIndex, message.c2TableIndex, message.c2ColumnIndex});
			}

			this.resultCollector.tell(new ResultCollector.ResultMessage(resultList));
		}
		this.busyWorkers.remove(message.getDependencyWorker());
		this.idleWorkers.add(message.getDependencyWorker());
		this.busyWorkList.remove(message.taskId);

		assignTasksToWorkers();

		if(this.workList.isEmpty() && this.busyWorkList.isEmpty()) {
			this.getContext().getLog().info("Number of INDs found total: "+this.allINDs.size());
			this.getContext().getLog().info("Number of assigned IND tasks during run: "+this.indTasks);
			this.getContext().getLog().info("Number of remaining pairs: "+this.pairs.size());
			ArrayList<Integer[]> notReadyPairs = new ArrayList<>();
			for(Integer[] pair: this.pairs) {
				if(pair[4] == 0 || pair[5] == 0)
					notReadyPairs.add(pair);
			}
			this.getContext().getLog().info("Number of pairs where at least one column not set ready: "+notReadyPairs.size());
			end();
		}

		return this;
	}

	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private Behavior<Message> handle(Terminated signal) {
		this.getContext().getLog().info("Worker terminated. Task will be placed at the front of the work list");
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		if(this.busyWorkers.contains(dependencyWorker)){
			this.busyWorkers.remove(dependencyWorker);
		} else {
			this.idleWorkers.remove(dependencyWorker);
		}
		int taskId = 0;
		for (Map.Entry<Integer, ActorRef<DependencyWorker.Message>> entry : this.busyWorkList.entrySet()) {
			if (entry.getValue().equals(dependencyWorker)){
				taskId = entry.getKey();
				break;
			}
		}
		this.workList.add(0, this.taskIdMap.get(taskId));
		return this;
	}

	private Behavior<Message> handle(ShutDownMinerMessage message) {
		Behaviors.stopped();
		return this;
	}

	private void assignTasksToWorkers(){
		while (!this.workList.isEmpty() && !this.idleWorkers.isEmpty()){
			this.getContext().getLog().info("unique column tasks: "+this.workList.stream().filter(task -> task instanceof UniqueColumnTask).count() + " and " +
					"IND tasks: "+this.workList.stream().filter(task -> task instanceof INDTask).count());
			WorkTask task = this.workList.get(0);
			this.workList.remove(0);
			ActorRef<DependencyWorker.Message> worker = this.idleWorkers.get(0);
			this.idleWorkers.remove(0);
			this.busyWorkers.add(worker);
			this.busyWorkList.put(task.id, worker);
			this.taskIdMap.put(task.id, task);

			int dataAmount = 0;
			if(task.getClass().equals(UniqueColumnTask.class)){
				LargeMessageProxy.LargeMessage msg = new DependencyWorker.UniqueColumnTaskMessage(this.largeMessageProxy, ((UniqueColumnTask)task));
				this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(msg, this.workers.get(worker)));
				dataAmount += ((UniqueColumnTask) task).getData().size();
			} else if(task.getClass().equals(INDTask.class)) {
				LargeMessageProxy.LargeMessage msg = new DependencyWorker.FindINDTaskMessage(this.largeMessageProxy, ((INDTask) task));
				this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(msg, this.workers.get(worker)));
				dataAmount += ((INDTask)task).getC1().size() + ((INDTask)task).getC2().size();
			}
			this.getContext().getLog().info("Assigning '" + String.valueOf(task.getClass())
					.substring(String.valueOf(task.getClass()).lastIndexOf(".") + 1) + "' with " +
					dataAmount + " entries to worker");
		}
	}
}