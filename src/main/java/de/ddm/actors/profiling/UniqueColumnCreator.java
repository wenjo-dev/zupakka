package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.actors.profiling.tasks.UniqueColumnTask;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

public class UniqueColumnCreator extends AbstractBehavior<UniqueColumnCreator.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable {
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CreateUniqueColumnMessage implements Message {
        private static final long serialVersionUID = 1739062314525633711L;
        ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
        ActorRef<DependencyWorker.Message> worker;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "uniqueColumnCreator";

    public static Behavior<Message> create(UniqueColumnTask task) {
        return Behaviors.setup(context -> new UniqueColumnCreator(context, task));
    }

    private UniqueColumnCreator(ActorContext<Message> context, UniqueColumnTask task) {
        super(context);
        this.task = task;
    }

    /////////////////
    // Actor State //
    /////////////////

    private final UniqueColumnTask task;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(CreateUniqueColumnMessage.class, this::handle)
                .build();
    }

    private Behavior<UniqueColumnCreator.Message> handle(UniqueColumnCreator.CreateUniqueColumnMessage message) {

        return this;
    }

}
