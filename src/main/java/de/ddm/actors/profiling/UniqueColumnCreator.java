package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.actors.profiling.tasks.UniqueColumnTask;
import de.ddm.configuration.SystemConfiguration;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;

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
        double size = this.task.getData().size();
        double redundancies = 0.0;
        double current = 0.0;
        double lastLogTime = System.currentTimeMillis();
        boolean checkReached = false;

        ArrayList<String> result = new ArrayList<>();
        for (String s: this.task.getData()){
            if(!result.contains(s))
                result.add(s);
            else
                redundancies += 1;
            current += 1;

            // log
            if(System.currentTimeMillis() - lastLogTime >= 1000) {
                this.getContext().getLog().info("table "+this.task.getTableIndex()+", column "+this.task.getColumnIndex()+": "+ (Math.round(current / size * 10000.0) / 100.0)+"% checked - redundancy quote: "+(Math.round(redundancies / current * 10000.0) / 100.0)+"%");
                lastLogTime = System.currentTimeMillis();
            }

            if(!checkReached && current >= 10000) {
                checkReached = true;
                if (redundancies / current >= SystemConfiguration.redundancyThreshold)
                    this.getContext().getLog().info("the first 10k values contain at least 20% redundant values - continue search for uniques");
                else {
                    this.getContext().getLog().info("the first 10k of values contain less than 20% redundant values - aborting search for uniques");
                    message.getWorker().tell(new DependencyWorker.UniqueColumnResultMessage(this.getContext().getSelf() , this.task.getData(), this.task.getTableIndex(),
                            this.task.getColumnIndex()));
                    return this;
                }
            }
        }
        message.getWorker().tell(new DependencyWorker.UniqueColumnResultMessage(this.getContext().getSelf(), result, this.task.getTableIndex(),
                this.task.getColumnIndex()));

        return this;
    }

}
