package ch.cmbntr.eventbus;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.DeadEvent;
import com.google.common.eventbus.Subscribe;

/**
 * Tests {@link EventBus}.
 * 
 * @author Michael Locher <cmbntr@gmail.com>
 */
public class EventBusTest {

  private EventBus testee;

  private ExecutorService exec;

  private ExecutorService provideExec() {
    return Executors.newCachedThreadPool();
  }

  private EventBus provideTestee(final ExecutorService exec) {
    return new EventBus("test-bus", false, exec, exec);
  }

  /**
   * Setup.
   */
  @Before
  public void setUp() {
    this.exec = provideExec();
    this.testee = provideTestee(this.exec);
  }

  /**
   * Shutdown the executor.
   * 
   * @throws InterruptedException if shutdown fails
   */
  @After
  public void shutdown() throws InterruptedException {
    this.exec.shutdown();
    this.exec.awaitTermination(1L, TimeUnit.MINUTES);
  }

  /**
   * Tests the bus.
   * 
   * @throws Exception if testing fails
   */
  @Test
  public void testBus() throws Exception {
    // *** arrange
    final B b1 = new B(testee);
    final B b2 = new B(testee);

    // *** act
    this.testee.register(b1);
    this.testee.register(b1);
    this.testee.registerWeak(b1);
    this.testee.registerWeak(b2);
    this.testee.post(new Y());
    this.testee.post("hello");
    this.testee.post(99);
    Thread.sleep(100L);
  }

  private static String receive(final String prefix, final Object target, final String thread, final Object event) {
    final String msg = String.format("%s: %s %s %s", prefix, target, thread, event);
    System.out.println(msg);
    return msg;
  }

  private static interface I {

    @Subscribe
    public void foo(CharSequence event);

  }

  private static class A implements I {

    @Override
    public synchronized void foo(final CharSequence event) {
      receive("foo", this, Thread.currentThread().getName(), event);
    }

    @Subscribe
    public void dead(final DeadEvent event) {
      receive("dead", this, Thread.currentThread().getName(), event);
    }

  }

  private static class B extends A {

    private EventBus cascadingBus;

    public B(EventBus cascadingBus) {
      super();
      this.cascadingBus = cascadingBus;
    }

    @Subscribe
    @AllowConcurrentEvents
    public void bar(final X event) {
      final String msg = receive("bar", this, Thread.currentThread().getName(), event);
      this.cascadingBus.post("<- cascading from " + msg);
    }

  }

  private static class X {
  }

  private static class Y extends X {
  }

}
