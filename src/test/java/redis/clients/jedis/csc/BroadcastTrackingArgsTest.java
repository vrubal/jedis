package redis.clients.jedis.csc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import redis.clients.jedis.exceptions.JedisException;

/**
 * Pure unit tests for the BCAST / NOLOOP tracking-mode wiring. These exercise
 * {@link CacheConfig}, {@link DefaultCache}, and the package-private
 * {@link CacheConnection#buildTrackingArgs(Cache)} helper without requiring a running Redis
 * instance.
 */
public class BroadcastTrackingArgsTest {

  @Test
  public void configDefaultsAreNonBroadcast() {
    CacheConfig cfg = CacheConfig.builder().build();
    assertFalse(cfg.isBroadcastMode());
    assertEquals(Collections.emptyList(), cfg.getPrefixes());
    assertFalse(cfg.noloop());
  }

  @Test
  public void configBuilderStoresBroadcastAndPrefixes() {
    CacheConfig cfg = CacheConfig.builder()
        .bcast()
        .prefixes("user:", "order:")
        .build();
    assertTrue(cfg.isBroadcastMode());
    assertEquals(Arrays.asList("user:", "order:"), cfg.getPrefixes());
  }

  @Test
  public void configPrefixesNullCoercedToEmptyList() {
    CacheConfig cfg = CacheConfig.builder()
        .bcast()
        .prefixes((List<String>) null)
        .build();
    assertEquals(Collections.emptyList(), cfg.getPrefixes());
  }

  @Test
  public void cacheReflectsConfigViaFactory() {
    CacheConfig cfg = CacheConfig.builder()
        .bcast()
        .prefixes("user:")
        .build();
    Cache cache = CacheFactory.getCache(cfg);
    assertTrue(cache.isBroadcastMode());
    assertEquals(Collections.singletonList("user:"), cache.getPrefixes());
  }

  @Test
  public void buildArgsForDefaultMode() {
    Cache cache = CacheFactory.getCache(CacheConfig.builder().build());
    String[] args = CacheConnection.buildTrackingArgs(cache);
    assertArrayEquals(new String[] { "TRACKING", "ON" }, args);
  }

  @Test
  public void buildArgsForBroadcastWithoutPrefixes() {
    Cache cache = CacheFactory.getCache(CacheConfig.builder().bcast().build());
    String[] args = CacheConnection.buildTrackingArgs(cache);
    assertArrayEquals(new String[] { "TRACKING", "ON", "BCAST" }, args);
  }

  @Test
  public void buildArgsForBroadcastWithPrefixes() {
    Cache cache = CacheFactory.getCache(CacheConfig.builder()
        .bcast()
        .prefixes("user:", "order:")
        .build());
    String[] args = CacheConnection.buildTrackingArgs(cache);
    assertArrayEquals(
        new String[] { "TRACKING", "ON", "BCAST", "PREFIX", "user:", "PREFIX", "order:" }, args);
  }

  @Test
  public void buildArgsRejectsEmptyPrefix() {
    Cache cache = CacheFactory.getCache(CacheConfig.builder()
        .bcast()
        .prefixes("user:", "")
        .build());
    assertThrows(JedisException.class, () -> CacheConnection.buildTrackingArgs(cache));
  }

  @Test
  public void buildArgsIgnoresPrefixesWhenBroadcastDisabled() {
    // Configuring prefixes without enabling broadcast mode should not leak PREFIX into the
    // handshake — the server would reject those args without BCAST.
    Cache cache = CacheFactory.getCache(CacheConfig.builder()
        .prefixes("user:")
        .build());
    String[] args = CacheConnection.buildTrackingArgs(cache);
    assertArrayEquals(new String[] { "TRACKING", "ON" }, args);
  }

  @Test
  public void configBuilderStoresNoLoop() {
    CacheConfig cfg = CacheConfig.builder().noloop().build();
    assertTrue(cfg.noloop());
  }

  @Test
  public void cacheReflectsNoLoopViaFactory() {
    Cache cache = CacheFactory.getCache(CacheConfig.builder().noloop().build());
    assertTrue(cache.isNoLoop());
  }

  @Test
  public void buildArgsForNoLoopOnly() {
    Cache cache = CacheFactory.getCache(CacheConfig.builder().noloop().build());
    String[] args = CacheConnection.buildTrackingArgs(cache);
    assertArrayEquals(new String[] { "TRACKING", "ON", "NOLOOP" }, args);
  }

  @Test
  public void buildArgsForBroadcastWithNoLoop() {
    Cache cache = CacheFactory.getCache(CacheConfig.builder()
        .bcast()
        .noloop()
        .build());
    String[] args = CacheConnection.buildTrackingArgs(cache);
    assertArrayEquals(new String[] { "TRACKING", "ON", "BCAST", "NOLOOP" }, args);
  }

  @Test
  public void buildArgsForBroadcastPrefixesAndNoLoop() {
    Cache cache = CacheFactory.getCache(CacheConfig.builder()
        .bcast()
        .prefixes("user:", "order:")
        .noloop()
        .build());
    String[] args = CacheConnection.buildTrackingArgs(cache);
    assertArrayEquals(
        new String[] { "TRACKING", "ON", "BCAST", "PREFIX", "user:", "PREFIX", "order:", "NOLOOP" },
        args);
  }
}
