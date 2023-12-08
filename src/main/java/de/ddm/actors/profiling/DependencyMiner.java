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
	private List<ActorRef<ColumnCreator.Message>> columnCreators;

	private int filesRead = 0;
	private final ArrayList<String[]>[] originalFileContents;
	// all columns with their origin [table-index][column-index] as key
	private final ArrayList<Map<Integer, ArrayList<String>>> columns = new ArrayList<>();

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

	private void createInternalINDTasks(int tableIndex) {
		this.getContext().getLog().info("creating internal IND task for "+tableIndex);
		for(int i = 0; i < this.columns.get(tableIndex).size(); i++) {
			for(int j = 0; j < this.columns.get(tableIndex).size(); j++) {
				if(i == j) continue;
				this.workList.add(new INDTask(this.columns.get(tableIndex).get(i), this.columns.get(tableIndex).get(j),
						tableIndex, i, tableIndex, j));
			}
		}
	}

	private Behavior<Message> handle(ColumnCreationMessage message) {
		this.getContext().getLog().info("Received " + message.taskList.size() + " columns from table " + message.taskList.get(0).getTableIndex());
		this.columnCreators.remove(message.columnCreator);
		this.filesRead += 1;
		// save columns
		for (UniqueColumnTask t: message.getTaskList()) {
			this.columns.get(t.getTableIndex()).put(t.getColumnIndex(), t.getData());
		}
		this.workList.addAll(message.getTaskList());

		assignTasksToWorkers();
		return this;
	}

	private Behavior<Message> handle(UniqueColumnToMinerMessage message) {
		this.getContext().getLog().info("Received " + message.getData().size() + " unique values for table " + this.inputFiles[message.tableIndex].getName() + " column " + this.headerLines[message.tableIndex][message.columnIndex]);
		double before = this.columns.get(message.tableIndex).get(message.columnIndex).size();
		this.getContext().getLog().info(message.getData().size() + " uniques of "+before+" - reduced to "+ ((double)message.getData().size()) / before * 100.0 + " percent");
		this.columns.get(message.tableIndex).put(message.columnIndex, message.data);
		this.busyWorkers.remove(message.getDependencyWorker());
		this.idleWorkers.add(message.getDependencyWorker());

		if (this.workList.isEmpty() && filesRead == this.inputFiles.length){
			for (int i = 0; i < filesRead; i++){
				createInternalINDTasks(i);
			}
		}

		assignTasksToWorkers();
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
		while (!this.workList.isEmpty() && !this.idleWorkers.isEmpty()){
			this.getContext().getLog().info("remaining tasks: "+this.workList.size());
			WorkTask task = this.workList.get(0);
			this.workList.remove(0);
			ActorRef<DependencyWorker.Message> worker = this.idleWorkers.get(0);
			this.idleWorkers.remove(0);
			this.busyWorkers.add(worker);
			if(task.getClass().equals(UniqueColumnTask.class)){
				LargeMessageProxy.LargeMessage msg = new DependencyWorker.UniqueColumnTaskMessage(this.largeMessageProxy, ((UniqueColumnTask)task));
				this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(msg, this.workers.get(worker)));
				this.getContext().getLog().info("Assigning Task '" + task.getClass() + "' to worker");
			} else if(task.getClass().equals(INDTask.class)) {
				// TODO message stuff
			}
		}
	}
}