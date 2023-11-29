package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.actors.profiling.tasks.UniqueColumnTask;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.*;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TaskMessage implements Message {
		private static final long serialVersionUID = -4667745204456518160L;
		ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
		int task;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class IdleStateMessage implements Message {
		private static final long serialVersionUID = -4667745203456218160L;
		ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RemoveDuplicatesMessage implements Message {
		private static final long serialVersionUID = -4667743782456218160L;
		ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
		UniqueColumnTask task;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyWorker::new);
	}

	private DependencyWorker(ActorContext<Message> context) {
		super(context);
		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.onMessage(IdleStateMessage.class, this::handle)
				.onMessage(RemoveDuplicatesMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf()));
		return this;
	}


	private Behavior<Message> handle(TaskMessage message) {
		this.getContext().getLog().info("Working on " + message.getTask());
		// I should probably know how to solve this task, but for now I just pretend some work...

		int result = 1;
		long time = System.currentTimeMillis();
		Random rand = new Random();
		int runtime = (rand.nextInt(2) + 2) * 1000;
		while (System.currentTimeMillis() - time < runtime)
			result = ((int) Math.abs(Math.sqrt(result)) * result) % 1334525;

		LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.CompletionMessage(this.getContext().getSelf(), result);
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, message.getDependencyMinerLargeMessageProxy()));

		return this;
	}

	private Behavior<Message> handle(IdleStateMessage message) {
		this.getContext().getLog().info("Checking for Work..");
		LargeMessageProxy.LargeMessage taskRequestMsg = new DependencyMiner.TaskRequestMessage(this.getContext().getSelf());
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(taskRequestMsg, message.getDependencyMinerLargeMessageProxy()));
		return this;
	}

	private Behavior<Message> handle(RemoveDuplicatesMessage message){
		this.getContext().getLog().info("Removing duplicates in table " + message.getTask().getTableIndex() + " column " + message.getTask().getColumnIndex());
		ArrayList<String> result = new ArrayList<>();
		String[] data = message.task.getData();
        for (String val : data) {
            if (!result.contains(val)) {
                result.add(val);
            }
        }
		this.getContext().getLog().info("Process finished. Sending results back to master..");
		LargeMessageProxy.LargeMessage taskFinishedMsg = new DependencyMiner.UniqueColumnTaskCompletedMessage(this.getContext().getSelf(), result, message.getTask().getTableIndex(), message.getTask().getColumnIndex());
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(taskFinishedMsg, message.getDependencyMinerLargeMessageProxy()));
		// Request more work..
		LargeMessageProxy.LargeMessage taskRequestMsg = new DependencyMiner.TaskRequestMessage(this.getContext().getSelf());
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(taskRequestMsg, message.getDependencyMinerLargeMessageProxy()));
		return this;
	}
}
