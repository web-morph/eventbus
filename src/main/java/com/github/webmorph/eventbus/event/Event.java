package com.github.webmorph.eventbus.event;

import com.github.webmorph.eventbus.EventBus;
import dev.ckateptb.reflection.Reflect;
import org.springframework.context.ApplicationContext;

/**
 * Base class for all events used in the reactive EventBus system.
 *
 * <p>This class serves as the foundation for both cancellable and non-cancellable event types.
 * It supports dynamic dispatching and static registration of handlers based on event type.</p>
 *
 * <p>Each subclass of {@code Event} can be dispatched via {@link #dispatch()}, and can also
 * register listeners via static {@code on(...)} methods without requiring direct access to the {@link EventBus}.</p>
 *
 * <p>Listeners are automatically associated with the calling event class using internal stack inspection.
 * This eliminates boilerplate code such as explicitly passing the event class for subscription.</p>
 *
 * @see CancelableEvent
 * @see EventHandler
 * @see EventPriority
 */
public abstract class Event {
    /**
     * Dispatches this event to all registered subscribers through the global {@link EventBus#GLOBAL}.
     *
     * @param <T> the actual runtime type of this event
     * @return the same event instance after all listeners have been processed
     */
    @SuppressWarnings("unchecked")
    public <T extends Event> T dispatch() {
        try {
            return Reflect.on("com.github.webmorph.provider.ApplicationContextProvider")
                    .getMethodWithNameAndParameters("getApplicationContext").orElseThrow().invoke()
                    .cast(ApplicationContext.class)
                    .getValue()
                    .getBean(EventBus.class).dispatchEvent((T) this);
        } catch (Exception e) {
            return EventBus.GLOBAL.dispatchEvent((T) this);
        }
    }
}
