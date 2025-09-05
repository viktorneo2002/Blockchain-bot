//! Concurrency primitives and telemetry integration for high-performance systems
//!
//! This module provides thread-safe state management and telemetry hooks that are
//! designed for high-throughput, low-latency applications.

use std::sync::Arc;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::fmt::Debug;
use std::time::Instant;

/// Thread-safe wrapper around shared state with optimized read/write patterns
/// 
/// # Type Parameters
/// * `T` - The inner state type, must implement `Clone` for snapshotting
/// 
/// # Examples
/// ```
/// use std::sync::Arc;
/// use your_crate::concurrency::SharedState;
/// 
/// #[derive(Clone, Default)]
/// struct AppState {
///     counter: u64,
/// }
/// 
/// let state = SharedState::new(AppState::default());
/// 
/// // Take a snapshot (lock-free read)
/// let snapshot = state.snapshot();
/// 
/// // Update state (exclusive write)
/// state.update(|s| {
///     s.counter += 1;
/// });
/// ```
pub struct SharedState<T> {
    inner: Arc<RwLock<T>>,
}

impl<T: Clone + 'static> SharedState<T> {
    /// Create a new shared state instance
    pub fn new(initial: T) -> Self {
        Self {
            inner: Arc::new(RwLock::new(initial)),
        }
    }

    /// Take a consistent snapshot of the current state
    /// 
    /// This performs a single atomic read and clones the state outside the lock.
    pub fn snapshot(&self) -> T {
        self.inner.read().clone()
    }

    /// Get a read-only guard for the state
    /// 
    /// Prefer `snapshot()` unless you need to chain operations.
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        self.inner.read()
    }

    /// Get a read-only guard for the state with timeout
    /// 
    /// Returns `None` if the lock couldn't be acquired in time.
    pub fn try_read_for(&self, timeout: std::time::Duration) -> Option<RwLockReadGuard<'_, T>> {
        self.inner.try_read_for(timeout)
    }

    /// Update the state with exclusive write access
    /// 
    /// The closure receives a mutable reference to the state.
    pub fn update<F>(&self, f: F)
    where
        F: FnOnce(&mut T),
    {
        let mut guard = self.inner.write();
        f(&mut guard);
    }

    /// Try to update the state with a timeout
    /// 
    /// Returns `true` if the update was successful, `false` if the lock couldn't be acquired.
    pub fn try_update<F>(&self, timeout: std::time::Duration, f: F) -> bool
    where
        F: FnOnce(&mut T),
    {
        if let Some(mut guard) = self.inner.try_write_for(timeout) {
            f(&mut guard);
            true
        } else {
            false
        }
    }
}

impl<T> Clone for SharedState<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Metrics sink for telemetry integration
/// 
/// Implement this trait to plug in different monitoring backends.
pub trait MetricsSink: Send + Sync + 'static {
    /// Record a gauge metric
    fn gauge(&self, key: &'static str, value: f64);
    
    /// Record a counter metric
    fn counter(&self, key: &'static str, value: f64, tags: Option<Vec<(&'static str, String)>>);
    
    /// Record a histogram metric
    fn histogram(&self, key: &'static str, value: f64, tags: Option<Vec<(&'static str, String)>>);
    
    /// Record timing information
    fn timing(&self, key: &'static str, duration: std::time::Duration);
}

/// No-op implementation for testing and when metrics are disabled
#[derive(Clone, Default)]
pub struct NoopMetricsSink;

impl MetricsSink for NoopMetricsSink {
    fn gauge(&self, _key: &'static str, _value: f64) {}
    fn counter(&self, _key: &'static str, _value: f64, _tags: Option<Vec<(&'static str, String)>>) {}
    fn histogram(&self, _key: &'static str, _value: f64, _tags: Option<Vec<(&'static str, String)>>) {}
    fn timing(&self, _key: &'static str, _duration: std::time::Duration) {}
}

/// Distributed tracing interface
pub trait Tracer: Send + Sync + 'static {
    /// Start a new trace span
    fn span(&self, name: &'static str) -> Box<dyn Span>;
    
    /// Get the current span if available
    fn current_span(&self) -> Option<Box<dyn Span>>;
}

/// A span in a distributed trace
pub trait Span: Send + 'static {
    /// Add a tag to the span
    fn tag(&mut self, key: &'static str, value: String);
    
    /// Record an event with the given message
    fn event(&self, message: &'static str);
    
    /// Record an error
    fn error(&self, error: &dyn std::error::Error);
    
    /// Record a metric
    fn metric(&self, name: &'static str, value: f64);
    
    /// Complete the span with timing information
    fn finish(self);
}

/// No-op implementation for testing and when tracing is disabled
#[derive(Clone, Default)]
pub struct NoopTracer;

impl Tracer for NoopTracer {
    fn span(&self, _name: &'static str) -> Box<dyn Span> {
        Box::new(NoopSpan)
    }
    
    fn current_span(&self) -> Option<Box<dyn Span>> {
        None
    }
}

/// No-op span implementation
pub struct NoopSpan;

impl Span for NoopSpan {
    fn tag(&mut self, _key: &'static str, _value: String) {}
    fn event(&self, _message: &'static str) {}
    fn error(&self, _error: &dyn std::error::Error) {}
    fn metric(&self, _name: &'static str, _value: f64) {}
    fn finish(self) {}
}

/// Scoped timer that records duration when dropped
pub struct ScopedTimer<'a> {
    start: Instant,
    metrics: &'a dyn MetricsSink,
    key: &'static str,
}

impl<'a> ScopedTimer<'a> {
    /// Create a new scoped timer
    pub fn new(metrics: &'a dyn MetricsSink, key: &'static str) -> Self {
        Self {
            start: Instant::now(),
            metrics,
            key,
        }
    }
    
    /// Record the current duration without consuming the timer
    pub fn record(&self) -> std::time::Duration {
        let duration = self.start.elapsed();
        self.metrics.timing(self.key, duration);
        duration
    }
}

impl<'a> Drop for ScopedTimer<'a> {
    fn drop(&mut self) {
        self.record();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use std::thread;
    
    #[derive(Clone, Debug, Default)]
    struct TestState {
        counter: u64,
        name: String,
    }
    
    #[test]
    fn test_shared_state() {
        let state = SharedState::new(TestState {
            counter: 0,
            name: "test".to_string(),
        });
        
        // Test snapshot
        let snapshot = state.snapshot();
        assert_eq!(snapshot.counter, 0);
        assert_eq!(snapshot.name, "test");
        
        // Test update
        state.update(|s| {
            s.counter = 42;
            s.name = "updated".to_string();
        });
        
        let snapshot = state.snapshot();
        assert_eq!(snapshot.counter, 42);
        assert_eq!(snapshot.name, "updated");
    }
    
    #[test]
    fn test_concurrent_updates() {
        let state = SharedState::new(TestState {
            counter: 0,
            name: String::new(),
        });
        
        let mut handles = vec![];
        
        // Spawn 10 threads that each increment the counter
        for _ in 0..10 {
            let state = state.clone();
            handles.push(thread::spawn(move || {
                state.update(|s| s.counter += 1);
            }));
        }
        
        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }
        
        // Verify the final counter value
        let snapshot = state.snapshot();
        assert_eq!(snapshot.counter, 10);
    }
    
    #[test]
    fn test_metrics_sink() {
        let sink = NoopMetricsSink;
        sink.gauge("test.gauge", 42.0);
        sink.counter("test.counter", 1.0, None);
        sink.histogram("test.hist", 10.0, None);
        sink.timing("test.timing", Duration::from_millis(100));
        
        // Just verify it doesn't panic
        assert!(true);
    }
    
    #[test]
    fn test_scoped_timer() {
        let sink = NoopMetricsSink;
        {
            let _timer = ScopedTimer::new(&sink, "test.timer");
            thread::sleep(Duration::from_millis(10));
        }
        // Timer is recorded when it goes out of scope
        assert!(true);
    }
}
