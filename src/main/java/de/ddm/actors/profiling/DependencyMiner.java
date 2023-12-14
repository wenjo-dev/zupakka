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
		boolean isDependant;
		int taskId;
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

	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
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
				this.workList.add(new INDTask(this.columns.get(pair[0]).get(pair[1]), this.columns.get(pair[2]).get(pair[3]),
						pair[0], pair[1], pair[2], pair[3]));
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
			createPairCandidates();
		}

		assignTasksToWorkers();
		return this;
	}

	private void createPairCandidates() {
		for(int i = 0; i < this.headerLines.length; i++) {
			for(int j = 0; j < this.headerLines[i].length; j++) {
				// loop through columns of current table
				for(int k = 0; k < this.headerLines[i].length; k++) {
					if (k == j)
						continue;
					// t1, c1, t2, c2, tc1 ready, tc2 ready
					this.pairs.add(new Integer[]{i, j, i, k, 0, 0});
				}
				// loop through other tables
				for(int k = 0; k < this.headerLines.length; k++) {
					if(k == i)
						continue;
					// loop through columns of other table
					for(int l = 0; l < this.headerLines[k].length; l++) {
						this.pairs.add(new Integer[]{i, j, k, l, 0, 0});
					}
				}
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

	private Behavior<Message> handle(INDToMinerMessage message) {
		if(message.isDependant()){
			this.getContext().getLog().info("Received IND: " + this.inputFiles[message.c1TableIndex].getName() + "." + this.headerLines[message.c1TableIndex][message.c1ColumnIndex] + " -> "
					+ this.inputFiles[message.c2TableIndex].getName() + "." + this.headerLines[message.c2TableIndex][message.c2ColumnIndex]);
			InclusionDependency ind = new InclusionDependency(this.inputFiles[message.getC2TableIndex()],
					new String[]{this.headerLines[message.getC2TableIndex()][message.c2ColumnIndex]},
					this.inputFiles[message.getC1TableIndex()],
					new String[]{this.headerLines[message.getC1TableIndex()][message.c1ColumnIndex]});
			ArrayList<InclusionDependency> resultList = new ArrayList<>();
			resultList.add(ind);
			this.resultCollector.tell(new ResultCollector.ResultMessage(resultList));
		}
		this.busyWorkers.remove(message.getDependencyWorker());
		this.idleWorkers.add(message.getDependencyWorker());
		this.busyWorkList.remove(message.taskId);

		assignTasksToWorkers();

		if(this.workList.isEmpty() && this.busyWorkList.isEmpty())
			end();

		return this;
	}

	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		if(this.busyWorkers.contains(dependencyWorker)){
			this.busyWorkers.remove(dependencyWorker);
		} else {
			this.idleWorkers.remove(dependencyWorker);
		}
		return this;
	}

	private void assignTasksToWorkers(){
		this.getContext().getLog().info(String.valueOf("BUSY TASKS: " + this.busyWorkList.size()));
		while (!this.workList.isEmpty() && !this.idleWorkers.isEmpty()){
			this.getContext().getLog().info("unique column tasks: "+this.workList.stream().filter(task -> task instanceof UniqueColumnTask).count() + " and " +
					"IND tasks: "+this.workList.stream().filter(task -> task instanceof INDTask).count());
			WorkTask task = this.workList.get(0);
			this.workList.remove(0);
			ActorRef<DependencyWorker.Message> worker = this.idleWorkers.get(0);
			this.idleWorkers.remove(0);
			this.busyWorkers.add(worker);

			this.busyWorkList.put(task.id, worker);

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