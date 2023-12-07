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
import de.ddm.actors.profiling.tasks.WorkTask;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Random;
import java.util.Set;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
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
	public static class UniqueColumnTaskMessage implements Message {
		private static final long serialVersionUID = -4667745204456518160L;
		ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
		UniqueColumnTask task;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class UniqueColumnResultMessage implements Message {
		private static final long serialVersionUID = -4661745204456512260L;
		ArrayList<String> data;
		int tableIndex;
		int columnIndex;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	private static ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;

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
				.onMessage(UniqueColumnTaskMessage.class, this::handle)
				.onMessage(UniqueColumnResultMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf(), this.largeMessageProxy));
		return this;
	}

	private Behavior<Message> handle(UniqueColumnTaskMessage message) {
		this.getContext().getLog().info("Received " + message.getTask().getClass() + " for " +  (message.getTask()).getData().length + " entries.");
		DependencyWorker.dependencyMinerLargeMessageProxy = message.getDependencyMinerLargeMessageProxy();
		ActorRef<UniqueColumnCreator.Message> actor = getContext().spawn(UniqueColumnCreator.create(message.getTask()),
				UniqueColumnCreator.DEFAULT_NAME + "_" + (message.getTask()).getTableIndex()
						+ ";" + (message.getTask()).getColumnIndex());
		actor.tell(new UniqueColumnCreator.CreateUniqueColumnMessage(this.getContext().getSelf()));
		return this;
	}

	private Behavior<Message> handle(UniqueColumnResultMessage message) {
		this.getContext().getLog().info("Found " + message.data.size() + " unique values in table " + message.tableIndex + " column " + message.getColumnIndex());
		LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.UniqueColumnToMinerMessage(this.getContext().getSelf(), message.getData(), message.getTableIndex(), message.getColumnIndex());
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, DependencyWorker.dependencyMinerLargeMessageProxy));
		return this;
	}
}
