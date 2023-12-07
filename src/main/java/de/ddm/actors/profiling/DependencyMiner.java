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
import de.ddm.actors.profiling.tasks.UniqueColumnTask;
import de.ddm.actors.profiling.tasks.WorkTask;
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
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		int result;
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
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		workers = new HashMap<>();

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
	private final ArrayList<String[]>[] originalFileContents = new ArrayList[7];

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
				.onMessage(CompletionMessage.class, this::handle)
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

	private Behavior<Message> handle(CompletionMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		// If this was a reasonable result, I would probably do something with it and potentially generate more work ... for now, let's just generate a random, binary IND.

		if (this.headerLines[0] != null) {
			Random random = new Random();
			int dependent = random.nextInt(this.inputFiles.length);
			int referenced = random.nextInt(this.inputFiles.length);
			File dependentFile = this.inputFiles[dependent];
			File referencedFile = this.inputFiles[referenced];
			String[] dependentAttributes = {this.headerLines[dependent][random.nextInt(this.headerLines[dependent].length)], this.headerLines[dependent][random.nextInt(this.headerLines[dependent].length)]};
			String[] referencedAttributes = {this.headerLines[referenced][random.nextInt(this.headerLines[referenced].length)], this.headerLines[referenced][random.nextInt(this.headerLines[referenced].length)]};
			InclusionDependency ind = new InclusionDependency(dependentFile, dependentAttributes, referencedFile, referencedAttributes);
			List<InclusionDependency> inds = new ArrayList<>(1);
			inds.add(ind);

			this.resultCollector.tell(new ResultCollector.ResultMessage(inds));
		}
		// I still don't know what task the worker could help me to solve ... but let me keep her busy.
		// Once I found all unary INDs, I could check if this.discoverNaryDependencies is set to true and try to detect n-ary INDs as well!

		//dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, 42));

		// At some point, I am done with the discovery. That is when I should call my end method. Because I do not work on a completable task yet, I simply call it after some time.
		if (System.currentTimeMillis() - this.startTime > 2000000)
			this.end();
		return this;
	}

	private Behavior<Message> handle(ColumnCreationMessage message) {
		this.getContext().getLog().info("Received " + message.taskList.size() + " columns from table " + message.taskList.get(0).getTableIndex());
		this.columnCreators.remove(message.columnCreator);
		this.workList.addAll(message.getTaskList());
		assignTasksToWorkers();
		return this;
	}

	private Behavior<Message> handle(UniqueColumnToMinerMessage message) {
		this.getContext().getLog().info("Received " + message.getData().size() + " values from worker");
		// TODO: SAVE STUFF

		// BUGGT RUM, ABER WEITER MACHEN HIER
		this.busyWorkers.remove(message.getDependencyWorker());
		this.idleWorkers.add(message.getDependencyWorker());
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
			WorkTask task = this.workList.get(0);
			this.workList.remove(0);
			ActorRef<DependencyWorker.Message> worker = this.idleWorkers.get(0);
			this.idleWorkers.remove(0);
			this.busyWorkers.add(worker);
			if(task.getClass().equals(UniqueColumnTask.class)){
				LargeMessageProxy.LargeMessage msg = new DependencyWorker.UniqueColumnTaskMessage(this.largeMessageProxy, ((UniqueColumnTask)task));
				this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(msg, this.workers.get(worker)));
				this.getContext().getLog().info("Assigning Task '" + task.getClass() + "' to worker.");
			}
		}
	}
}