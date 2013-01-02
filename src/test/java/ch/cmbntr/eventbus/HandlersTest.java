package ch.cmbntr.eventbus;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.Set;

import org.junit.Test;

import com.google.common.collect.Iterables;

/**
 * Tests {@link Handlers}.
 * 
 * @author Michael Locher <cmbntr@gmail.com>
 */
public class HandlersTest {

  /**
   * Tests {@link Handlers#linearizeHierarchy(Class)}.
   */
  @Test
  public void testLinearizeHierarchy() {
    final Set<Class<?>> f1 = Handlers.linearizeHierarchy(ArrayList.class);
    final Set<Class<?>> f2 = Handlers.linearizeHierarchy(ArrayList.class);
    assertSame(f1, f2);
    assertEquals(10, f1.size());
    assertEquals(ArrayList.class, Iterables.get(f1, 0));
  }

}
