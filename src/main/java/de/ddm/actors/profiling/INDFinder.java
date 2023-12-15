package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.actors.profiling.tasks.INDTask;
import de.ddm.actors.profiling.tasks.UniqueColumnTask;
import de.ddm.configuration.SystemConfiguration;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;

public class INDFinder extends AbstractBehavior<INDFinder.Message> {

    ////////////////////
    // Actor Messages //
    ////////////////////

    public interface Message extends AkkaSerializable {
    }

    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FindINDMessage implements INDFinder.Message {
        private static final long serialVersionUID = 1700062314525633711L;
        ActorRef<DependencyWorker.Message> worker;
    }

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "indFinder";

    public static Behavior<Message> create(INDTask task) {
        return Behaviors.setup(context -> new INDFinder(context, task));
    }

    private INDFinder(ActorContext<Message> context, INDTask task) {
        super(context);
        this.task = task;
    }

    /////////////////
    // Actor State //
    /////////////////

    private final INDTask task;

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive<Message> createReceive() {
        return newReceiveBuilder()
                .onMessage(FindINDMessage.class, this::handle)
                .build();
    }

    private Behavior<INDFinder.Message> handle(INDFinder.FindINDMessage message) {
        boolean firstDependant = true;
        boolean secondDependant = true;
        HashMap<String, Boolean> mapFirst = new HashMap<>();
        for (int i = 0; i < this.task.getC1().size(); i++){
            mapFirst.put(this.task.getC1().get(i), true);
        }
        for (int i = 0; i < this.task.getC2().size(); i++){
            if(mapFirst.get(this.task.getC2().get(i)) == null){
                firstDependant = false;
                break;
            }
        }
        mapFirst = null;
        HashMap<String, Boolean> mapSecond = new HashMap<>();
        for (int i = 0; i < this.task.getC2().size(); i++){
            mapSecond.put(this.task.getC2().get(i), true);
        }
        for (int i = 0; i < this.task.getC1().size(); i++){
            if(mapSecond.get(this.task.getC1().get(i)) == null){
                secondDependant = false;
                break;
            }
        }

        message.getWorker().tell(new DependencyWorker.INDTaskResultMessage(this.getContext().getSelf(), task.getC1TableIndex(), task.getC1ColumnIndex(),
                task.getC2TableIndex(), task.getC2ColumnIndex(), firstDependant, secondDependant, this.task.id));

        return this;
    }

}
