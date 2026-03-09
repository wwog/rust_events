use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;


static NEXT_ID: AtomicU64 = AtomicU64::new(1);

fn next_id() -> u64 {
    NEXT_ID.fetch_add(1, Ordering::Relaxed)
}

type Callback<T> = Box<dyn Fn(&T) + Send + Sync + 'static>;

struct CallbackEntry<T> {
    id: u64,
    kind: CallbackKind<T>,
}

enum CallbackKind<T> {
    Persistent(Callback<T>),
    Once {
        fired: AtomicBool,
        cb: Callback<T>,
    },
}


/// `bind` / `bind_once` 返回的取消订阅句柄。
///
/// 调用 [`Dispose::dispose`] 后，对应的回调会从 [`Emitter`] 中移除，
/// 之后的 `fire` 不再触发该回调。
///
/// 句柄本身持有 `Arc`，可以跨线程传递和存储。
pub struct Dispose<T> {
    id: u64,
    callbacks: std::sync::Weak<RwLock<Vec<CallbackEntry<T>>>>,
}

impl<T> Dispose<T> {
    fn new(id: u64, callbacks: &Arc<RwLock<Vec<CallbackEntry<T>>>>) -> Self {
        Self {
            id,
            callbacks: Arc::downgrade(callbacks),
        }
    }

    /// 取消订阅，从 [`Emitter`] 中移除对应的回调。
    /// - 若 [`Emitter`] 已被销毁，`Weak::upgrade()` 返回 `None`，静默退出，不持有任何资源。
    /// - 若回调已被移除（`bind_once` 触发后、或已调用过 `dispose`），同样无副作用。
    pub fn dispose(self) {
        if let Some(callbacks) = self.callbacks.upgrade() {
            callbacks.write().retain(|e| e.id != self.id);
        }
    }
}


/// `emitter.event()` 返回的链式注册句柄。
///
/// ```
/// use events::Emitter;
///
/// let emitter = Emitter::<i32>::new();
///
/// let d1 = emitter.event().bind(|val| println!("持续监听: {val}"));
/// let d2 = emitter.event().bind_once(|val| println!("只收一次: {val}"));
///
/// emitter.fire(1); // 两个回调都触发，d2 自动失效
/// emitter.fire(2); // 只有 d1 触发
///
/// d1.dispose();    // 主动取消 d1
/// emitter.fire(3); // 无回调触发
/// ```
pub struct Event<T> {
    callbacks: Arc<RwLock<Vec<CallbackEntry<T>>>>,
}

impl<T> Event<T> {
    fn new(callbacks: Arc<RwLock<Vec<CallbackEntry<T>>>>) -> Self {
        Self { callbacks }
    }

    /// 注册一个持续监听的回调，每次 `fire` 都会触发。
    /// 返回 [`Dispose`] 句柄，可用于主动取消订阅。
    pub fn bind(self, callback: impl Fn(&T) + Send + Sync + 'static) -> Dispose<T> {
        let id = next_id();
        self.callbacks.write().push(CallbackEntry {
            id,
            kind: CallbackKind::Persistent(Box::new(callback)),
        });
        Dispose::new(id, &self.callbacks)
    }

    /// 注册一个只触发一次的回调，触发后自动销毁。
    /// 返回 [`Dispose`] 句柄，也可在触发前主动取消。
    pub fn bind_once(self, callback: impl Fn(&T) + Send + Sync + 'static) -> Dispose<T> {
        let id = next_id();
        self.callbacks.write().push(CallbackEntry {
            id,
            kind: CallbackKind::Once {
                fired: AtomicBool::new(false),
                cb: Box::new(callback),
            },
        });
        Dispose::new(id, &self.callbacks)
    }
}


pub struct Emitter<T> {
    callbacks: Arc<RwLock<Vec<CallbackEntry<T>>>>,
}

impl<T: Send + Sync + 'static> Default for Emitter<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Send + Sync + 'static> Emitter<T> {
    pub fn new() -> Self {
        Self {
            callbacks: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// 返回一个 [`Event`] 句柄，用于链式注册回调。
    pub fn event(&self) -> Event<T> {
        Event::new(Arc::clone(&self.callbacks))
    }

    /// 触发事件，依次执行所有回调。
    /// - `bind` 回调每次都触发。
    /// - `bind_once` 回调恰好触发一次（并发安全），触发后自动移除。
    pub fn fire(&self, event: T) {
        {
            let callbacks = self.callbacks.read();
            for entry in callbacks.iter() {
                match &entry.kind {
                    CallbackKind::Persistent(cb) => cb(&event),
                    CallbackKind::Once { fired, cb } => {
                        if fired
                            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                        {
                            cb(&event);
                        }
                    }
                }
            }
        }

        // 清理已触发的 Once 条目
        self.callbacks.write().retain(|entry| match &entry.kind {
            CallbackKind::Persistent(_) => true,
            CallbackKind::Once { fired, .. } => !fired.load(Ordering::Acquire),
        });
    }

    /// 移除所有已注册的回调。
    pub fn clear(&self) {
        self.callbacks.write().clear();
    }

    /// 返回当前已注册的回调数量。
    pub fn len(&self) -> usize {
        self.callbacks.read().len()
    }

    /// 返回当前是否已注册回调。
    pub fn is_empty(&self) -> bool {
        self.callbacks.read().is_empty()
    }
}

// ─── 测试 ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::thread;

    #[test]
    fn test_bind_persistent() {
        let emitter = Emitter::<i32>::new();
        let counter = Arc::new(Mutex::new(0));

        let c = Arc::clone(&counter);
        emitter.event().bind(move |val| {
            *c.lock().unwrap() += val;
        });

        emitter.fire(1);
        emitter.fire(1);
        emitter.fire(1);

        assert_eq!(*counter.lock().unwrap(), 3);
    }

    #[test]
    fn test_bind_once() {
        let emitter = Emitter::<i32>::new();
        let counter = Arc::new(Mutex::new(0));

        let c = Arc::clone(&counter);
        emitter.event().bind_once(move |val| {
            *c.lock().unwrap() += val;
        });

        assert_eq!(emitter.len(), 1);

        emitter.fire(1);
        assert_eq!(*counter.lock().unwrap(), 1);
        assert_eq!(emitter.len(), 0, "bind_once 触发后应被移除");

        emitter.fire(1);
        assert_eq!(*counter.lock().unwrap(), 1, "第二次 fire 不应再触发");
    }

    #[test]
    fn test_dispose_persistent() {
        let emitter = Emitter::<i32>::new();
        let counter = Arc::new(Mutex::new(0));

        let c = Arc::clone(&counter);
        let dispose = emitter.event().bind(move |val| {
            *c.lock().unwrap() += val;
        });

        emitter.fire(1);
        assert_eq!(*counter.lock().unwrap(), 1);

        dispose.dispose();

        emitter.fire(1);
        emitter.fire(1);
        assert_eq!(*counter.lock().unwrap(), 1, "dispose 后不应再触发");
        assert_eq!(emitter.len(), 0);
    }

    #[test]
    fn test_dispose_once_before_fire() {
        let emitter = Emitter::<i32>::new();
        let counter = Arc::new(Mutex::new(0));

        let c = Arc::clone(&counter);
        let dispose = emitter.event().bind_once(move |_| {
            *c.lock().unwrap() += 1;
        });

        dispose.dispose(); // 触发前取消

        emitter.fire(1);
        assert_eq!(*counter.lock().unwrap(), 0, "dispose 后 bind_once 不应触发");
    }

    #[test]
    fn test_dispose_idempotent() {
        // dispose 后回调已移除，再次 dispose 无副作用（不 panic）
        let emitter = Emitter::<i32>::new();
        let d1 = emitter.event().bind(|_| {});
        let d2 = emitter.event().bind(|_| {});

        assert_eq!(emitter.len(), 2);
        d1.dispose();
        assert_eq!(emitter.len(), 1);
        d2.dispose();
        assert_eq!(emitter.len(), 0);
    }

    #[test]
    fn test_multiple_binds_independent_dispose() {
        let emitter = Emitter::<i32>::new();
        let c1 = Arc::new(Mutex::new(0));
        let c2 = Arc::new(Mutex::new(0));

        let a = Arc::clone(&c1);
        let b = Arc::clone(&c2);

        let d1 = emitter.event().bind(move |v| *a.lock().unwrap() += v);
        let _d2 = emitter.event().bind(move |v| *b.lock().unwrap() += v);

        emitter.fire(1);
        assert_eq!(*c1.lock().unwrap(), 1);
        assert_eq!(*c2.lock().unwrap(), 1);

        d1.dispose(); // 只取消 d1

        emitter.fire(1);
        assert_eq!(*c1.lock().unwrap(), 1, "d1 已 dispose，不应再触发");
        assert_eq!(*c2.lock().unwrap(), 2, "d2 仍活跃，应继续触发");
    }

    #[test]
    fn test_concurrent_fire() {
        let emitter = Arc::new(Emitter::<i32>::new());
        let counter = Arc::new(Mutex::new(0_i32));

        let c = Arc::clone(&counter);
        emitter.event().bind(move |val| {
            *c.lock().unwrap() += val;
        });

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let e = Arc::clone(&emitter);
                thread::spawn(move || e.fire(1))
            })
            .collect();

        for h in handles {
            h.join().expect("子线程 panic");
        }

        assert_eq!(*counter.lock().unwrap(), 4);
    }

    #[test]
    fn test_concurrent_bind_once_fires_exactly_once() {
        use std::sync::atomic::{AtomicI32, Ordering};

        let emitter = Arc::new(Emitter::<i32>::new());
        let call_count = Arc::new(AtomicI32::new(0));

        let c = Arc::clone(&call_count);
        emitter.event().bind_once(move |_| {
            c.fetch_add(1, Ordering::Relaxed);
        });

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let e = Arc::clone(&emitter);
                thread::spawn(move || e.fire(1))
            })
            .collect();

        for h in handles {
            h.join().expect("子线程 panic");
        }

        assert_eq!(
            call_count.load(Ordering::Relaxed),
            1,
            "bind_once 并发 fire 下应恰好执行一次"
        );
        assert_eq!(emitter.len(), 0, "执行后应被清理");
    }

    #[test]
    fn test_dispose_from_another_thread() {
        let emitter = Arc::new(Emitter::<i32>::new());
        let counter = Arc::new(Mutex::new(0));

        let c = Arc::clone(&counter);
        let dispose = emitter.event().bind(move |val| {
            *c.lock().unwrap() += val;
        });

        emitter.fire(1);
        assert_eq!(*counter.lock().unwrap(), 1);

        // 在另一个线程中 dispose
        thread::spawn(move || dispose.dispose())
            .join()
            .expect("子线程 panic");

        emitter.fire(1);
        assert_eq!(*counter.lock().unwrap(), 1, "跨线程 dispose 后不应再触发");
    }

    #[test]
    fn test_dispose_after_emitter_dropped() {
        let (dispose, emitter) = {
            let emitter = Emitter::<i32>::new();
            let dispose = emitter.event().bind(|val| println!("收到: {val}"));
            (dispose, emitter)
        };
        emitter.fire(1);
        dispose.dispose();
        emitter.fire(2);
    }

    #[test]
    fn test_clear() {
        let emitter = Emitter::<i32>::new();

        emitter.event().bind(|_| {});
        emitter.event().bind(|_| {});
        emitter.event().bind_once(|_| {});

        assert_eq!(emitter.len(), 3);
        emitter.clear();
        assert_eq!(emitter.len(), 0);
    }

    #[test]
    fn test_struct_event() {
        struct UserInfo {
            id: u32,
            name: String,
        }

        enum AppEvent {
            UserCreated(UserInfo),
            UserDeleted(u32),
        }

        let emitter = Emitter::<AppEvent>::new();

        emitter.event().bind(|ev| match ev {
            AppEvent::UserCreated(u) => println!("创建用户: id={}, name={}", u.id, u.name),
            AppEvent::UserDeleted(id) => println!("删除用户: id={id}"),
        });

        emitter.fire(AppEvent::UserCreated(UserInfo {
            id: 1,
            name: "Alice".to_string(),
        }));
        emitter.fire(AppEvent::UserDeleted(1));
    }
}
