package ch.cmbntr.eventbus;

/**
 * An event handler.
 * 
 * @author Michael Locher <cmbntr@gmail.com>
 */
public interface Handler {

  /**
   * Dispatches the given event.
   * 
   * @param event the event to dispatch
   * @return {@code true} if the event was dispatched, or {@code false} if no target was available
   */
  public boolean dispatch(Object event);

  /**
   * Registers a target with a strong reference.
   * 
   * @param target the target
   */
  public void register(Object target);

  /**
   * Registers a target with a weak reference.
   * 
   * @param target the target
   */
  public void registerWeak(Object target);

  /**
   * Deregisters a target.
   * 
   * @param target the target
   */
  public void unregister(Object target);

}