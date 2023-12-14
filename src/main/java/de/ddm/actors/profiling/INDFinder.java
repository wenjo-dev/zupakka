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
        double lastLogTime = System.currentTimeMillis();

        boolean isDependant = true;
        ArrayList<String> firstColumn = this.task.getC1();
        ArrayList<String> secondColumn = this.task.getC2();

        for(int i = 0; i < secondColumn.size(); i++) {
            // log
            if(System.currentTimeMillis() - lastLogTime >= 1000) {
                this.getContext().getLog().info((Math.round((double)i / (double)secondColumn.size() * 10000.0) / 100.0)+"% checked");
                lastLogTime = System.currentTimeMillis();
            }

            if(!firstColumn.contains(secondColumn.get(i))) {
                isDependant = false;
                break;
            }
        }
        message.getWorker().tell(new DependencyWorker.INDTaskResultMessage(this.getContext().getSelf(), task.getC1TableIndex(), task.getC1ColumnIndex(),
                task.getC2TableIndex(), task.getC2ColumnIndex(), isDependant, this.task));

        return this;
    }

}
