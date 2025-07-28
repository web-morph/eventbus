package com.github.webmorph.eventbus;

import com.github.webmorph.eventbus.annotation.EventHandler;
import com.github.webmorph.eventbus.event.CancelableEvent;
import com.github.webmorph.eventbus.event.Event;
import com.github.webmorph.eventbus.event.EventPriority;
import com.github.webmorph.eventbus.listener.Listener;
import dev.ckateptb.reflection.Reflect;
import dev.ckateptb.reflection.parameter.ReflectParameter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.util.Collection;

/**
 * Reactive, priority-based event bus built on top of Reactor Core.
 *
 * <p>This class provides a publish-subscribe mechanism for dispatching and handling
 * asynchronous events in a non-blocking, backpressure-aware way. Events are published
 * to an internal {@link reactor.core.publisher.Sinks.Many} and dispatched to all
 * subscribed handlers based on their {@link EventPriority}.</p>
 *
 * <p>Supports both manually registered handlers via {@link #on(Class, EventPriority, boolean, com.github.webmorph.eventbus.event.EventHandler)}
 * and declarative registration via {@link EventHandler}
 * annotated methods in {@link Listener} components.</p>
 *
 * <p>Cancelable events are skipped for non-forced handlers once cancelled.</p>
 */
@Slf4j
public class EventBus {
    /**
     * Global EventBus static instance
     */
    public static final EventBus GLOBAL = new EventBus();
    /**
     * Sink used to publish incoming events to subscribers.
     * Events are emitted as {@code (priority, event)} tuples to support ordered handling.
     */
    private final Sinks.Many<Tuple2<EventPriority, Event>> events = Sinks.many().multicast().onBackpressureBuffer();
    /**
     * Shared Flux stream for all event emissions, consumed by handlers.
     */
    private final Flux<Tuple2<EventPriority, Event>> handlers = this.events.asFlux();

    /**
     * Dispatches the given event to all handlers in order of {@link EventPriority}.
     *
     * <p>Each priority level receives a separate emission of the same event,
     * allowing handlers at different levels to react in isolation. Cancelable events
     * may prevent further propagation depending on handler configuration.</p>
     *
     * @param event the event instance to dispatch
     * @param <E>   the type of the event
     * @return the same event instance, for chaining or inspection
     */
    public <E extends Event> E dispatchEvent(E event) {
        for (EventPriority priority : EventPriority.values()) {
            this.events.emitNext(Tuples.of(priority, event), (signalType, emitResult) -> signalType == SignalType.ON_NEXT);
        }
        return event;
    }

    /**
     * Subscribes a handler to events of a specific type and priority.
     *
     * <p>When an event of the given type is dispatched with the specified priority,
     * the provided handler will be invoked. Handlers may choose to ignore cancelled events
     * by setting {@code ignoreCancelled} to {@code true}.</p>
     *
     * @param eventClass      the class of the event to listen for
     * @param priority        the priority level at which the handler should be invoked
     * @param ignoreCancelled whether to skip {@link CancelableEvent}s that are cancelled
     * @param handler         the logic to execute when the event occurs
     * @param <E>             the type of the event
     * @return a {@link Disposable} that can be used to unregister the handler
     */
    public <E extends Event> Disposable on(Class<E> eventClass, EventPriority priority, boolean ignoreCancelled, com.github.webmorph.eventbus.event.EventHandler<E> handler) {
        return this.handlers
                .filter(tuple -> tuple.getT1().equals(priority))
                .map(Tuple2::getT2)
                .filter(event -> event.getClass().isAssignableFrom(eventClass))
                .filter(event -> ignoreCancelled || !(event instanceof CancelableEvent cancelable) || !cancelable.isCanceled())
                .cast(eventClass)
                .subscribe(handler::handle);
    }

    /**
     * Subscribes a handler to events of a specific type with default priority ({@link EventPriority#NORMAL}).
     *
     * @param eventClass      the class of the event to listen for
     * @param ignoreCancelled whether to skip {@link CancelableEvent}s that are cancelled
     * @param handler         the event handling logic
     * @param <E>             the type of the event
     * @return a {@link Disposable} to unsubscribe the handler
     */
    public <E extends Event> Disposable on(Class<E> eventClass, boolean ignoreCancelled, com.github.webmorph.eventbus.event.EventHandler<E> handler) {
        return this.on(eventClass, EventPriority.NORMAL, ignoreCancelled, handler);
    }

    /**
     * Subscribes a handler to events of a specific type and priority,
     * excluding cancelled events if applicable.
     *
     * @param eventClass the class of the event to listen for
     * @param priority   the priority level for the handler
     * @param handler    the event handling logic
     * @param <E>        the type of the event
     * @return a {@link Disposable} to unsubscribe the handler
     */
    public <E extends Event> Disposable on(Class<E> eventClass, EventPriority priority, com.github.webmorph.eventbus.event.EventHandler<E> handler) {
        return this.on(eventClass, priority, false, handler);
    }

    /**
     * Subscribes a handler to events of a specific type with default settings.
     *
     * <p>Uses {@link EventPriority#NORMAL} and excludes cancelled events.</p>
     *
     * @param eventClass the class of the event to listen for
     * @param handler    the event handling logic
     * @param <E>        the type of the event
     * @return a {@link Disposable} to unsubscribe the handler
     */
    public <E extends Event> Disposable on(Class<E> eventClass, com.github.webmorph.eventbus.event.EventHandler<E> handler) {
        return this.on(eventClass, EventPriority.NORMAL, false, handler);
    }

    /**
     * Initializes the event bus after the application context is started by scanning
     * all beans implementing {@link Listener} and registering their {@link EventHandler} methods.
     *
     * <p>This method uses reflection to locate all methods annotated with {@code @EventHandler},
     * validates them, and subscribes them to the appropriate event types based on their
     * parameter type and annotation metadata.</p>
     *
     * @param context the application context
     */
    @SuppressWarnings("unchecked")
    private void init(ApplicationContext context) {
        Class<EventHandler> annotation = EventHandler.class;
        context.getBeansOfType(Listener.class, true, false).values()
                .forEach(listener -> Reflect.on(listener).getMethodsWithAnnotation(annotation)
                        .forEach(method -> {
                            Collection<ReflectParameter<?>> parameters = method.getParameters();
                            if (parameters.size() != 1) {
                                log.warn("Illegal arguments count or the provided argument is not an event. {}", method.getRaw().toGenericString());
                                return;
                            }
                            parameters.stream()
                                    .map(ReflectParameter::getType)
                                    .findFirst()
                                    .ifPresent(type -> {
                                        if (Event.class.isAssignableFrom(type)) {
                                            EventHandler handler = method.getAnnotation(annotation);
                                            on((Class<? extends Event>) type, handler.priority(),
                                                    handler.force(), method::invoke);
                                        }
                                    });
                        }));
    }
}
